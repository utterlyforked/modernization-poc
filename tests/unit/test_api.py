import datetime

import psycopg2
import pytest

import api as api_module


class FakeCursor:
    def __init__(self, fetch=None):
        self.calls = []
        self.fetch = fetch if fetch is not None else {"id": 42}

    def execute(self, sql, params=None):
        self.calls.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self.fetch

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.cur = FakeCursor()
        self.commits = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1

    def close(self):
        pass


@pytest.fixture
def db(monkeypatch):
    """Fake legacy + new-system connections; records which tenants were opened."""
    state = type("State", (), {})()
    state.legacy = FakeConn()
    state.new = FakeConn()
    state.legacy_tenants = []

    def legacy(tenant_id):
        state.legacy_tenants.append(tenant_id)
        return state.legacy

    monkeypatch.setattr(api_module, "get_legacy_conn", legacy)
    monkeypatch.setattr(api_module, "get_new_system_conn", lambda: state.new)
    return state


@pytest.fixture
def client():
    api_module.app.config["TESTING"] = True
    return api_module.app.test_client()


BODY = {"firstname": "Ann", "surname": "Lee", "date_of_birth": "1990-01-01",
        "city": "Oslo", "extra_field": "xtra"}


@pytest.mark.parametrize("tenant, column, params", [
    ("tenant_a1", "data_2", ("Ann", "Lee", "1990-01-01", "Oslo", None, "xtra", None)),
    ("tenant_a2", "data_2", ("Ann", "Lee", "1990-01-01", "Oslo", None, "xtra", None)),
    ("tenant_b1", "data_c", ("Ann", "Lee", "1990-01-01", "Oslo", None, None, "xtra")),
    ("tenant_b2", "data_c", ("Ann", "Lee", "1990-01-01", "Oslo", None, None, "xtra")),
])
def test_post_routes_extra_field_by_legacy_type(client, db, tenant, column, params):
    resp = client.post("/api/persons", json={**BODY, "tenant_id": tenant})
    assert resp.status_code == 200
    assert resp.get_json()["legacy_id"] == 42
    assert resp.get_json()["tenant_id"] == tenant
    assert db.legacy_tenants == [tenant]
    (sql, got), = db.legacy.cur.calls
    assert sql.startswith("INSERT INTO person")
    # a-type has data_1/2/3, b-type has data_a/b/c
    assert ("data_1, data_2, data_3" in sql) == (column == "data_2")
    assert ("data_a, data_b, data_c" in sql) == (column == "data_c")
    assert got == params
    assert db.legacy.commits == 1


def test_post_empty_extra_field_is_stored_as_null(client, db):
    client.post("/api/persons", json={**BODY, "tenant_id": "tenant_a1", "extra_field": ""})
    assert db.legacy.cur.calls[0][1][5] is None


@pytest.mark.parametrize("missing", ["tenant_id", "firstname", "surname"])
def test_post_missing_required_field_is_400(client, db, missing):
    body = {**BODY, "tenant_id": "tenant_a1"}
    del body[missing]
    resp = client.post("/api/persons", json=body)
    assert resp.status_code == 400
    assert db.legacy_tenants == []


def test_post_unknown_tenant_is_400(client, db):
    resp = client.post("/api/persons", json={**BODY, "tenant_id": "tenant_zz"})
    assert resp.status_code == 400
    assert "Unknown tenant" in resp.get_json()["error"]
    assert db.legacy_tenants == []


@pytest.mark.parametrize("body", [
    {"legacy_id": 1},
    {"tenant_id": "tenant_a1"},
    {},
])
def test_put_requires_tenant_and_legacy_id(client, db, body):
    resp = client.put("/api/persons", json=body)
    assert resp.status_code == 400
    assert db.legacy_tenants == []


def test_put_unknown_tenant_is_400(client, db):
    resp = client.put("/api/persons", json={"tenant_id": "nope", "legacy_id": 1})
    assert resp.status_code == 400


@pytest.mark.parametrize("tenant, column", [("tenant_a2", "data_2"), ("tenant_b2", "data_c")])
def test_put_updates_type_specific_column(client, db, tenant, column):
    resp = client.put("/api/persons", json={**BODY, "tenant_id": tenant, "legacy_id": 7})
    assert resp.status_code == 200
    (sql, params), = db.legacy.cur.calls
    assert sql.startswith("UPDATE person")
    assert f"{column} = %s" in sql
    assert params == ("Ann", "Lee", "1990-01-01", "Oslo", "xtra", 7)
    assert db.new.cur.calls == []  # no modernized_only -> new system untouched


def test_put_without_extra_field_writes_empty_string(client, db):
    client.put("/api/persons", json={"tenant_id": "tenant_a1", "legacy_id": 7})
    assert db.legacy.cur.calls[0][1][4] == ""


def test_put_with_modernized_only_updates_new_system(client, db):
    resp = client.put("/api/persons", json={**BODY, "tenant_id": "tenant_b1", "legacy_id": 7,
                                            "modernized_only": "2024-02-03"})
    assert resp.status_code == 200
    (sql, params), = db.new.cur.calls
    assert sql.startswith("UPDATE person SET modernized_only")
    assert params == ("2024-02-03", "tenant_b1", 7)
    assert db.new.commits == 1


def test_health_lists_tenants(client):
    assert client.get("/api/health").get_json()["tenants"] == [
        "tenant_a1", "tenant_a2", "tenant_b1", "tenant_b2"]


# --- pipeline events -------------------------------------------------------------------------------------

class FakeEvents:
    def __init__(self):
        self.emitted = []
        self.error = None

    def emit(self, stage, message, **kwargs):
        if self.error:
            raise self.error
        self.emitted.append((stage, kwargs.get("severity", "info"), message, kwargs))


@pytest.fixture(autouse=True)
def events(monkeypatch):
    fake = FakeEvents()
    monkeypatch.setattr(api_module, "events", fake)
    return fake


def test_post_emits_an_api_event_for_the_record(client, db, events):
    client.post("/api/persons", json={**BODY, "tenant_id": "tenant_a1"})
    (stage, severity, message, kwargs), = events.emitted
    assert (stage, severity, message) == ("api", "info", "tenant_a1 person#42 INSERT written to legacy")
    assert (kwargs["tenant_id"], kwargs["legacy_id"]) == ("tenant_a1", 42)


def test_put_emits_an_api_event_for_the_record(client, db, events):
    client.put("/api/persons", json={**BODY, "tenant_id": "tenant_b1", "legacy_id": 9})
    (stage, _, message, kwargs), = events.emitted
    assert (stage, message) == ("api", "tenant_b1 person#9 UPDATE written to legacy")
    assert kwargs["legacy_id"] == 9


def test_validation_failure_emits_a_warning(client, db, events):
    assert client.post("/api/persons", json={"tenant_id": "tenant_a1"}).status_code == 400
    assert [(stage, severity) for stage, severity, _, _ in events.emitted] == [("api", "warn")]


def test_write_failure_emits_an_error(client, db, events):
    db.legacy.cur.execute = lambda *a, **k: (_ for _ in ()).throw(psycopg2.OperationalError("down"))
    assert client.post("/api/persons", json={**BODY, "tenant_id": "tenant_a1"}).status_code == 500
    assert [(stage, severity) for stage, severity, _, _ in events.emitted] == [("api", "error")]


def test_event_write_failure_does_not_affect_the_request(client, db, events):
    events.error = RuntimeError("pipeline_event does not exist")
    resp = client.post("/api/persons", json={**BODY, "tenant_id": "tenant_a1"})
    assert resp.status_code == 200
    assert resp.get_json()["legacy_id"] == 42
    assert db.legacy.commits == 1


class QueryCursor:
    """Returns canned rows; records the query. `results` maps a SQL fragment to fetchone() rows."""
    def __init__(self, rows=(), results=None, error=None):
        self.rows, self.results, self.error = list(rows), results or {}, error
        self.calls = []
        self.current = None

    def execute(self, sql, params=None):
        self.calls.append((" ".join(sql.split()), params))
        self.current = None
        if self.error:
            raise self.error
        for fragment, value in self.results.items():
            if fragment in sql:
                if isinstance(value, Exception):
                    raise value
                self.current = {"v": value}

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.current

    def close(self):
        pass


class QueryConn(FakeConn):
    autocommit = False

    def __init__(self, cursor):
        super().__init__()
        self.cur = cursor


def event_row(id, severity="info", tenant="tenant_a1"):
    return {"id": id, "ts": datetime.datetime(2024, 5, 1, 12, 0, id), "source": "api", "stage": "api",
            "severity": severity, "tenant_id": tenant, "legacy_id": 1, "message": f"m{id}", "detail": None}


def use_cursor(monkeypatch, cursor):
    monkeypatch.setattr(api_module, "get_new_system_conn", lambda: QueryConn(cursor))
    return cursor


def test_events_first_page_is_newest_events_oldest_first(client, monkeypatch):
    cur = use_cursor(monkeypatch, QueryCursor([event_row(3), event_row(2)]))  # DESC from the database
    body = client.get("/api/events?limit=2").get_json()
    sql, params = cur.calls[0]
    assert "ORDER BY id DESC" in sql and params == [0, 2]
    assert [e["id"] for e in body] == [2, 3]
    assert body[0]["ts"] == "2024-05-01T12:00:02"


def test_events_since_id_pages_forward(client, monkeypatch):
    cur = use_cursor(monkeypatch, QueryCursor([event_row(8), event_row(9)]))
    body = client.get("/api/events?since_id=7").get_json()
    sql, params = cur.calls[0]
    assert "id > %s" in sql and "ORDER BY id ASC" in sql and params == [7, 200]
    assert [e["id"] for e in body] == [8, 9]


def test_events_filters_by_severity_and_tenant(client, monkeypatch):
    cur = use_cursor(monkeypatch, QueryCursor())
    client.get("/api/events?since_id=1&severity=warn,error&tenant=tenant_b2&limit=5")
    sql, params = cur.calls[0]
    assert "severity = ANY(%s)" in sql and "tenant_id = %s" in sql
    assert params == [1, ["warn", "error"], "tenant_b2", 5]


def test_events_limit_is_capped(client, monkeypatch):
    cur = use_cursor(monkeypatch, QueryCursor())
    client.get("/api/events?limit=999999")
    assert cur.calls[0][1][-1] == 1000


def test_events_missing_table_is_an_empty_list(client, monkeypatch):
    use_cursor(monkeypatch, QueryCursor(error=psycopg2.errors.UndefinedTable("no table")))
    assert client.get("/api/events").get_json() == []


def test_pipeline_status_combines_connectors_and_database(client, monkeypatch):
    use_cursor(monkeypatch, QueryCursor(results={
        "person_staging": 4, "person_dead_letter": psycopg2.errors.UndefinedTable("x"),
        "source = 'cdc-consumer'": datetime.datetime(2024, 5, 1, 12, 0, 0),
        "severity = 'error'": 2, "interval '1 minute'": 11}))
    monkeypatch.setattr(api_module, "connector_states", lambda: {"legacy-a1-connector": "RUNNING"})
    body = client.get("/api/pipeline/status").get_json()
    assert body["connectors"] == {"legacy-a1-connector": "RUNNING"}
    assert body["staging_backlog"] == 4
    assert body["dead_letters"] is None  # table does not exist yet; the rest still reported
    assert body["last_consumer_activity"] == "2024-05-01T12:00:00"
    assert (body["errors"], body["events_last_minute"]) == (2, 11)


def test_pipeline_status_reports_unreachable_debezium(client, monkeypatch):
    use_cursor(monkeypatch, QueryCursor())

    def unreachable():
        raise OSError("connection refused")

    monkeypatch.setattr(api_module, "connector_states", unreachable)
    assert client.get("/api/pipeline/status").get_json()["connectors"] == {"debezium": "UNREACHABLE"}


def run_monitor(sequence):
    """Run monitor_connectors over a sequence of states() results (dict, or Exception to raise)."""
    items = iter(sequence)

    def states():
        item = next(items)
        if isinstance(item, Exception):
            raise item
        return item

    api_module.monitor_connectors(states=states, sleep=lambda s: None, iterations=len(sequence))


def test_monitor_emits_only_on_state_change(events):
    running, failed = {"c1": "RUNNING"}, {"c1": "FAILED"}
    run_monitor([running, running, failed, failed, running])
    assert [(sev, msg) for _, sev, msg, _ in events.emitted] == [
        ("error", "c1: RUNNING -> FAILED"), ("info", "c1: FAILED -> RUNNING")]
    assert {stage for stage, _, _, _ in events.emitted} == {"connector"}


def test_monitor_reports_a_connector_first_seen_failing(events):
    run_monitor([{"c1": "FAILED"}])
    assert [(sev, msg) for _, sev, msg, _ in events.emitted] == [("error", "c1: unknown -> FAILED")]


def test_monitor_reports_debezium_unreachable_once(events):
    run_monitor([{"c1": "RUNNING"}, OSError("refused"), OSError("refused")])
    assert [(sev, msg) for _, sev, msg, _ in events.emitted] == [
        ("error", "debezium: unknown -> UNREACHABLE")]
