"""Happy-path e2e tests: automated version of TESTING.md scenarios 1-7."""
import pytest

from conftest import TENANTS, wait_until

pytestmark = pytest.mark.e2e

LEGACY_A = ["tenant_a1", "tenant_a2"]


def wait_for_person(api, tenant_id, legacy_id, predicate=lambda p: True):
    def check():
        person = api.find_person(tenant_id, legacy_id)
        return person if person and predicate(person) else None
    return wait_until(check, desc=f"{tenant_id}/{legacy_id} in /api/persons")


def legacy_payload(tenant_id, surname, extra):
    base = {"firstname": "Direct", "surname": surname, "date_of_birth": "1990-01-01", "city": "Legacyville"}
    if tenant_id in LEGACY_A:
        return {**base, "data_1": "one", "data_2": extra, "data_3": "three"}
    return {**base, "data_a": "aaa", "data_b": "bbb", "data_c": extra}


@pytest.mark.parametrize("tenant_id", ["tenant_a1", "tenant_b1"])
def test_direct_legacy_insert_syncs(api, unique_surname, tenant_id):
    """Row written straight to a legacy DB shows up with the right tenant and mapped extra_field."""
    legacy_id = api.create_legacy(tenant_id, legacy_payload(tenant_id, unique_surname, "mapped-value"))

    person = wait_for_person(api, tenant_id, legacy_id)
    assert person["tenant_id"] == tenant_id
    assert person["surname"] == unique_surname
    assert person["extra_field"] == "mapped-value"  # data_2 (A) / data_c (B)


@pytest.mark.parametrize("tenant_id", TENANTS)
def test_post_writes_legacy_and_syncs(api, unique_surname, tenant_id):
    r = api.create_person({
        "tenant_id": tenant_id, "firstname": "Post", "surname": unique_surname,
        "date_of_birth": "1995-06-15", "city": "Postville", "extra_field": "via-api",
    })
    assert r.status_code == 200
    legacy_id = r.json()["legacy_id"]

    assert api.get_legacy(tenant_id, legacy_id)["surname"] == unique_surname

    person = wait_for_person(api, tenant_id, legacy_id)
    assert person["legacy_id"] == legacy_id
    assert person["firstname"] == "Post"
    assert person["extra_field"] == "via-api"


@pytest.mark.parametrize("tenant_id", TENANTS)
def test_put_propagates_to_new_system(api, unique_surname, tenant_id):
    legacy_id = api.create_person({
        "tenant_id": tenant_id, "firstname": "Before", "surname": unique_surname, "city": "Old", "extra_field": "old",
    }).json()["legacy_id"]
    wait_for_person(api, tenant_id, legacy_id)

    r = api.update_person({
        "tenant_id": tenant_id, "legacy_id": legacy_id, "firstname": "After",
        "surname": unique_surname, "date_of_birth": "2000-02-02", "city": "New", "extra_field": "new",
    })
    assert r.status_code == 200

    person = wait_for_person(api, tenant_id, legacy_id, lambda p: p["firstname"] == "After")
    assert (person["city"], person["extra_field"]) == ("New", "new")


def test_same_named_person_in_two_tenants_stays_two_rows(api, unique_surname):
    ids = {t: api.create_person({"tenant_id": t, "firstname": "Twin", "surname": unique_surname}).json()["legacy_id"]
           for t in ("tenant_a1", "tenant_b1")}
    for tenant_id, legacy_id in ids.items():
        wait_for_person(api, tenant_id, legacy_id)

    rows = [p for p in api.get_persons() if p["surname"] == unique_surname]
    assert sorted(p["tenant_id"] for p in rows) == ["tenant_a1", "tenant_b1"]


def test_put_does_not_touch_other_tenant_with_same_legacy_id(api, unique_surname):
    """PUT to tenant_a1 using a legacy_id that belongs to tenant_a2 must leave a2's row alone."""
    id_a1 = api.create_person({"tenant_id": "tenant_a1", "firstname": "Orig-A1", "surname": unique_surname}).json()["legacy_id"]
    id_a2 = api.create_person({"tenant_id": "tenant_a2", "firstname": "Orig-A2", "surname": unique_surname}).json()["legacy_id"]
    wait_for_person(api, "tenant_a1", id_a1)
    wait_for_person(api, "tenant_a2", id_a2)

    api.update_person({"tenant_id": "tenant_a1", "legacy_id": id_a2, "firstname": "Hijack", "surname": unique_surname})

    # Marker update on a1's own row: once it arrives, CDC has had time to deliver any stray a2 change.
    api.update_person({"tenant_id": "tenant_a1", "legacy_id": id_a1, "firstname": "Marker", "surname": unique_surname})
    wait_for_person(api, "tenant_a1", id_a1, lambda p: p["firstname"] == "Marker")

    assert api.find_person("tenant_a2", id_a2)["firstname"] == "Orig-A2"


def test_unknown_tenant_returns_400(api):
    assert api.create_person({"tenant_id": "tenant_nope", "firstname": "X", "surname": "Y"}).status_code == 400
    assert api.update_person({"tenant_id": "tenant_nope", "legacy_id": 1, "firstname": "X", "surname": "Y"}).status_code == 400


@pytest.mark.parametrize("payload", [
    {"firstname": "X", "surname": "Y"},
    {"tenant_id": "tenant_a1", "surname": "Y"},
    {"tenant_id": "tenant_a1", "firstname": "X"},
])
def test_post_missing_required_fields_returns_400(api, payload):
    assert api.create_person(payload).status_code == 400


def test_put_missing_required_fields_returns_400(api):
    assert api.update_person({"tenant_id": "tenant_a1"}).status_code == 400
    assert api.update_person({"legacy_id": 1}).status_code == 400
