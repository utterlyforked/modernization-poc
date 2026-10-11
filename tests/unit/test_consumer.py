import json
import types

import psycopg2
import pytest

import consumer
import pipeline_events
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


class FakeEvents:
    def __init__(self, error=None):
        self.error = error
        self.emitted = []

    def emit(self, stage, message, **kwargs):
        if self.error:
            raise self.error
        self.emitted.append((stage, kwargs.get("severity", "info"), message, kwargs))


def make_consumer():
    c = consumer.CDCConsumer.__new__(consumer.CDCConsumer)  # skip Kafka/DB connect
    c.db_conn = FakeConn()
    c.events = FakeEvents()
    c.attempts = {}
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


def test_stage_record_with_none_row_rolls_back_and_raises():
    c = make_consumer()
    with pytest.raises(AttributeError):
        c.stage_record("tenant_a1", None, "DELETE")
    assert c.db_conn.rollbacks == 1
    assert c.db_conn.commits == 0


class FakeMsg:
    def __init__(self, topic, value, partition=0, offset=7):
        self._topic, self._value, self._partition, self._offset = topic, value, partition, offset

    def topic(self):
        return self._topic

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None


class FakeKafka:
    """Delivers the given messages, then raises KeyboardInterrupt to end consume_messages()."""
    def __init__(self, msgs):
        self.msgs = list(msgs)
        self.committed = []
        self.seeks = []

    def poll(self, timeout=None):
        if not self.msgs:
            raise KeyboardInterrupt
        return self.msgs.pop(0)

    def commit(self, msg):
        self.committed.append(msg)

    def seek(self, tp):
        self.seeks.append(tp)

    def close(self):
        pass


def run_loop(msg, staging_error=None, dead_letter_error=None, events_error=None):
    """Run consume_messages() over one message; the staging insert (and optionally the dead-letter insert) fails."""
    c = make_consumer()
    c.db_conn.close = lambda: None
    execute = c.db_conn.cur.execute

    def failing_execute(sql, params=None):
        execute(sql, params)
        if staging_error and "INSERT INTO person_staging" in sql:
            raise staging_error
        if dead_letter_error and "person_dead_letter" in sql:
            raise dead_letter_error

    c.db_conn.cur.execute = failing_execute
    c.events.error = events_error
    c.consumer = FakeKafka([msg])
    c.consume_messages()
    return c


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
    monkeypatch.setattr(consumer.time, "sleep", lambda s: None)


MSG = FakeMsg("legacy_a1.public.person", event("c", after={"id": 1, "firstname": "A"}))


def dead_letter_inserts(c):
    return [params for sql, params in c.db_conn.cur.calls if "person_dead_letter" in sql]


def test_offset_is_committed_after_successful_staging():
    c = run_loop(MSG)
    assert c.consumer.committed == [MSG]
    assert c.consumer.seeks == []
    assert dead_letter_inserts(c) == []


def test_offset_is_not_committed_when_the_database_fails():
    c = run_loop(MSG, staging_error=psycopg2.OperationalError("server closed the connection"))
    assert c.consumer.committed == []
    assert c.consumer.seeks == [("legacy_a1.public.person", 0, 7)]  # rewound, will be delivered again
    assert dead_letter_inserts(c) == []


def test_unstageable_message_is_dead_lettered_then_committed():
    c = run_loop(MSG, staging_error=psycopg2.DataError("value too long for type character varying(100)"))
    (params,) = dead_letter_inserts(c)
    assert params[:3] == ("legacy_a1.public.person", 0, 7)
    assert "DataError" in params[4]
    assert c.consumer.committed == [MSG]


def test_delete_event_is_dead_lettered_not_silently_lost():
    delete = FakeMsg("legacy_a1.public.person", event("d", after=None, before={"id": 5}))
    c = run_loop(delete)
    (params,) = dead_letter_inserts(c)
    assert "AttributeError" in params[4]
    assert c.consumer.committed == [delete]


def test_unparseable_message_is_dead_lettered_then_committed():
    junk = FakeMsg("legacy_a1.public.person", b"not json")
    c = run_loop(junk)
    assert dead_letter_inserts(c)[0][3] == "not json"
    assert c.consumer.committed == [junk]


def test_offset_is_not_committed_when_dead_lettering_fails():
    c = run_loop(MSG, staging_error=psycopg2.DataError("bad"), dead_letter_error=psycopg2.OperationalError("down"))
    assert c.consumer.committed == []
    assert c.consumer.seeks == [("legacy_a1.public.person", 0, 7)]


def stages(c):
    return [(stage, severity) for stage, severity, _, _ in c.events.emitted]


def test_staged_message_emits_received_then_staged_for_the_record():
    c = run_loop(MSG)
    assert stages(c) == [("consumer.received", "info"), ("consumer.staged", "info")]
    _, _, message, kwargs = c.events.emitted[1]
    assert message == "tenant_a1 person#1 INSERT staged in new system (person_staging)"
    assert (kwargs["tenant_id"], kwargs["legacy_id"]) == ("tenant_a1", 1)


def test_database_failure_emits_a_retry_warning_with_attempt_count():
    c = run_loop(MSG, staging_error=psycopg2.OperationalError("server closed the connection"))
    assert stages(c) == [("consumer.received", "info"), ("consumer.retry", "warn")]
    assert c.events.emitted[1][3]["detail"]["attempt"] == 1


def test_dead_letter_emits_an_error_with_payload():
    c = run_loop(MSG, staging_error=psycopg2.DataError("too long"))
    assert ("consumer.dead_letter", "error") in stages(c)
    detail = c.events.emitted[-1][3]["detail"]
    assert "DataError" in detail["error"] and detail["offset"] == 7 and detail["raw_value"]


@pytest.mark.parametrize("kwargs", [
    {},
    {"staging_error": psycopg2.DataError("bad")},
])
def test_event_write_failure_does_not_change_staging_or_offset_commits(kwargs):
    expected = run_loop(MSG, **kwargs)
    broken = run_loop(MSG, events_error=RuntimeError("pipeline_event does not exist"), **kwargs)
    assert broken.consumer.committed == expected.consumer.committed == [MSG]
    assert broken.consumer.seeks == expected.consumer.seeks == []
    assert broken.db_conn.cur.calls == expected.db_conn.cur.calls
    assert broken.db_conn.commits == expected.db_conn.commits


def test_event_write_failure_does_not_stop_retry_rewind():
    c = run_loop(MSG, staging_error=psycopg2.OperationalError("down"), events_error=RuntimeError("no table"))
    assert c.consumer.committed == []
    assert c.consumer.seeks == [("legacy_a1.public.person", 0, 7)]


def test_dbt_failure_emits_an_error_event_with_output(monkeypatch):
    c = make_consumer()
    result = types.SimpleNamespace(returncode=1, stdout="Compilation Error", stderr="boom")
    monkeypatch.setattr(consumer.subprocess, "run", lambda *a, **k: result)
    c.run_dbt_transformation()
    assert stages(c) == [("dbt", "info"), ("dbt", "error")]
    assert c.events.emitted[-1][3]["detail"]["stdout"] == "Compilation Error"


def test_dbt_success_emits_finished_event(monkeypatch):
    c = make_consumer()
    c.db_conn.cur.fetchall = lambda: [("tenant_a1", 1), ("tenant_a1", 2), ("tenant_b2", 7)]
    monkeypatch.setattr(consumer.subprocess, "run",
                        lambda *a, **k: types.SimpleNamespace(returncode=0, stdout="", stderr=""))
    c.run_dbt_transformation()
    assert stages(c) == [("dbt", "info"), ("dbt", "info")]
    _, _, message, kwargs = c.events.emitted[-1]
    assert kwargs["detail"]["rows"] == 3
    assert "3 staged row(s) merged into person (tenant_a1 person#1, tenant_a1 person#2, tenant_b2 person#7)" in message


def test_event_writer_swallows_failures_and_backs_off(monkeypatch):
    attempts = []

    def failing_connect(**kwargs):
        attempts.append(kwargs)
        raise psycopg2.OperationalError("down")

    monkeypatch.setattr(pipeline_events.psycopg2, "connect", failing_connect)
    writer = pipeline_events.EventWriter({"host": "x"}, "test")
    writer.emit("api", "one")
    writer.emit("api", "two")  # within RETRY_INTERVAL: no second connection attempt
    assert len(attempts) == 1


def test_event_writer_inserts_and_prunes_to_bound_growth(monkeypatch):
    conn = FakeConn()
    conn.autocommit = False
    monkeypatch.setattr(pipeline_events.psycopg2, "connect", lambda **kwargs: conn)
    writer = pipeline_events.EventWriter({"host": "x"}, "test")
    writer.emit("api", "hello", severity="warn", tenant_id="tenant_a1", legacy_id=4, detail={"k": "v"})
    sqls = [sql for sql, _ in conn.cur.calls]
    assert conn.autocommit is True
    assert any("CREATE TABLE IF NOT EXISTS pipeline_event" in sql for sql in sqls)
    insert = next(params for sql, params in conn.cur.calls if "INSERT INTO pipeline_event" in sql)
    assert insert == ("test", "api", "warn", "tenant_a1", 4, "hello", '{"k": "v"}')
    assert any("DELETE FROM pipeline_event" in sql for sql in sqls)
