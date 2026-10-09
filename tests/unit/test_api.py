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
