import json

import pytest

import consumer
from consumer import convert_date, parse_event


def event(op, after=None, before=None, wrapped=True):
    payload = {"op": op, "after": after, "before": before}
    return json.dumps({"payload": payload} if wrapped else payload).encode()


@pytest.mark.parametrize("value, expected", [
    (None, None),
    (0, "1970-01-01"),
    (19000, "2022-01-08"),
    ("1990-05-17", "1990-05-17"),
])
def test_convert_date(value, expected):
    assert convert_date(value) == expected


@pytest.mark.parametrize("op, expected", [
    ("c", "INSERT"), ("u", "UPDATE"), ("d", "DELETE"), ("r", "INSERT"), ("x", "INSERT"),
])
def test_operation_mapping(op, expected):
    _, operation, _ = parse_event("legacy_a1.public.person", event(op, after={"id": 1}))
    assert operation == expected


def test_missing_op_defaults_to_insert():
    raw = json.dumps({"payload": {"after": {"id": 1}}}).encode()
    assert parse_event("legacy_a1.public.person", raw)[1] == "INSERT"


@pytest.mark.parametrize("topic, tenant", sorted(consumer.TENANT_MAPPING.items()))
def test_topic_to_tenant(topic, tenant):
    assert parse_event(topic, event("c", after={"id": 1}))[0] == tenant


def test_unknown_topic_returns_none():
    assert parse_event("other.public.person", event("c", after={"id": 1})) is None


def test_payload_wrapper_and_bare_envelope_are_equivalent():
    after = {"id": 7, "firstname": "Ann"}
    topic = "legacy_b1.public.person"
    assert (parse_event(topic, event("c", after=after, wrapped=True))
            == parse_event(topic, event("c", after=after, wrapped=False))
            == ("tenant_b1", "INSERT", after))


def test_accepts_str_input():
    raw = event("c", after={"id": 1}).decode()
    assert parse_event("legacy_a1.public.person", raw)[2] == {"id": 1}


def test_row_is_after_image():
    after = {"id": 3, "firstname": "A", "data_2": "x", "date_of_birth": 19000}
    assert parse_event("legacy_a2.public.person", event("u", after=after))[2] == after


@pytest.mark.xfail(strict=True, reason=(
    "Bug (ticket 03, bug 2): a delete has `after: null`, so row is None and "
    "stage_record raises AttributeError (swallowed), silently dropping the delete. "
    "Expected: the row comes from `before`."))
def test_delete_event_carries_the_deleted_row():
    before = {"id": 5, "firstname": "Gone"}
    _, operation, row = parse_event("legacy_a1.public.person",
                                    event("d", after=None, before=before))
    assert operation == "DELETE"
    assert row == before


def test_delete_event_current_behaviour_is_row_none():
    row = parse_event("legacy_a1.public.person",
                      event("d", after=None, before={"id": 5}))[2]
    assert row is None


class FakeCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.cur = FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def make_consumer():
    c = consumer.CDCConsumer.__new__(consumer.CDCConsumer)  # skip Kafka/DB connect
    c.db_conn = FakeConn()
    return c


def test_stage_record_inserts_converted_row():
    c = make_consumer()
    c.stage_record("tenant_a1", {"id": 9, "firstname": "A", "surname": "B",
                                 "date_of_birth": 19000, "city": "X", "data_2": "d2"}, "UPDATE")
    (sql, params), = c.db_conn.cur.calls
    assert "INSERT INTO person_staging" in sql
    assert params == ("tenant_a1", 9, "person", "UPDATE", "A", "B", "2022-01-08", "X",
                      None, "d2", None, None, None, None)
    assert c.db_conn.commits == 1


def test_stage_record_with_none_row_rolls_back_without_raising():
    c = make_consumer()
    c.stage_record("tenant_a1", None, "DELETE")
    assert c.db_conn.rollbacks == 1
    assert c.db_conn.commits == 0


class FakeMsg:
    def __init__(self, topic, value):
        self._topic, self._value = topic, value

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def error(self):
        return None


class FakeKafka:
    """Delivers the given messages, then raises KeyboardInterrupt to end consume_messages()."""
    def __init__(self, msgs):
        self.msgs = list(msgs)
        self.committed = []

    def poll(self, timeout=None):
        if not self.msgs:
            raise KeyboardInterrupt
        return self.msgs.pop(0)

    def commit(self, msg):
        self.committed.append(msg)

    def close(self):
        pass


@pytest.mark.xfail(strict=True, reason=(
    "Issue #12: stage_record catches any DB error, rolls back and returns normally, and the main loop "
    "then commits the offset anyway, so the event is lost with nothing to retry. "
    "Expected: the offset is committed only after a successful stage."))
def test_offset_is_not_committed_when_staging_fails():
    c = make_consumer()
    c.db_conn.close = lambda: None
    c.db_conn.cur.execute = lambda sql, params=None: (_ for _ in ()).throw(
        ValueError("value too long for type character varying(100)"))  # not a dead connection
    msg = FakeMsg("legacy_a1.public.person", event("c", after={"id": 1, "firstname": "A"}))
    c.consumer = FakeKafka([msg])

    c.consume_messages()

    assert c.db_conn.rollbacks == 1  # staging really did fail
    assert c.consumer.committed == []


def test_offset_is_committed_after_successful_staging():
    c = make_consumer()
    c.db_conn.close = lambda: None
    msg = FakeMsg("legacy_a1.public.person", event("c", after={"id": 1, "firstname": "A"}))
    c.consumer = FakeKafka([msg])

    c.consume_messages()

    assert c.consumer.committed == [msg]
