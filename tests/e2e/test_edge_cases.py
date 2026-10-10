"""E2E edge cases for the suspected pipeline bugs listed in issue #4.

Tests that fail here are findings; once confirmed against a real run they get `xfail(strict=True, reason=...)`.
Run via `make test-e2e` (Docker only); DB access is direct on the compose network.
"""
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from conftest import TENANTS, as_date, connect_legacy, connect_new_system, legacy_payload, wait_for_person

pytestmark = pytest.mark.e2e


def create_synced(api, tenant_id, surname, **fields):
    payload = {"tenant_id": tenant_id, "firstname": "Start", "surname": surname, "city": "Origin", **fields}
    legacy_id = api.create_person(payload).json()["legacy_id"]
    wait_for_person(api, tenant_id, legacy_id)
    return legacy_id


def put(api, tenant_id, legacy_id, surname, **fields):
    r = api.update_person({"tenant_id": tenant_id, "legacy_id": legacy_id, "surname": surname, "firstname": "Start", **fields})
    assert r.status_code == 200
    return r


def wait_for_cdc_barrier(api, tenant_id, surname):
    """Write a marker row to the tenant and wait for it: earlier events in that topic have been consumed by then.

    Only valid per tenant: ordering is guaranteed within a topic, not across tenants.
    """
    marker = api.create_legacy(tenant_id, legacy_payload(tenant_id, surname, "barrier"))
    wait_for_person(api, tenant_id, marker)


# --- 1. modernized_only ----------------------------------------------------------------------------------------

@pytest.mark.poisons_stack
@pytest.mark.parametrize("tenant_id", ["tenant_a1", "tenant_b2"])
def test_modernized_only_survives_legacy_update(api, unique_surname, tenant_id):
    legacy_id = create_synced(api, tenant_id, unique_surname)
    put(api, tenant_id, legacy_id, unique_surname, modernized_only="2024-05-01")
    wait_for_person(api, tenant_id, legacy_id, lambda p: as_date(p["modernized_only"]) == "2024-05-01")

    # Legacy-side update (no knowledge of modernized_only) and wait for it to be re-synced.
    assert api.update_legacy(tenant_id, legacy_id, {"firstname": "LegacyEdit", "surname": unique_surname}).status_code == 200
    person = wait_for_person(api, tenant_id, legacy_id, lambda p: p["firstname"] == "LegacyEdit")

    assert as_date(person["modernized_only"]) == "2024-05-01"


@pytest.mark.poisons_stack
def test_modernized_only_survives_rapid_updates(api, unique_surname):
    tenant_id = "tenant_a2"
    legacy_id = create_synced(api, tenant_id, unique_surname)
    for i in range(1, 6):
        put(api, tenant_id, legacy_id, unique_surname, firstname=f"Rapid{i}", modernized_only=f"2024-06-0{i}")

    wait_for_person(api, tenant_id, legacy_id, lambda p: p["firstname"] == "Rapid5")
    time.sleep(5)  # let any remaining echoes of earlier updates land
    person = api.find_person(tenant_id, legacy_id)
    assert person["firstname"] == "Rapid5"
    assert as_date(person["modernized_only"]) == "2024-06-05"


# --- 2. DELETE events ------------------------------------------------------------------------------------------

@pytest.mark.xfail(strict=True, reason="issue #4: DELETE events have a null `after`; staging fails with "
                   "\"'NoneType' object has no attribute 'get'\", the event goes to person_dead_letter and the row stays in person")
@pytest.mark.parametrize("tenant_id", ["tenant_a1", "tenant_b1"])
def test_legacy_delete_removes_row_from_person(api, unique_surname, tenant_id):
    legacy_id = create_synced(api, tenant_id, unique_surname)

    conn = connect_legacy(tenant_id)
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM person WHERE id = %s", (legacy_id,))
    conn.close()

    wait_for_cdc_barrier(api, tenant_id, f"{unique_surname}-barrier")
    time.sleep(3)  # allow a dbt run after the delete event
    assert api.find_person(tenant_id, legacy_id) is None, "legacy row was deleted but is still in person"


# --- 3. lost-update race ---------------------------------------------------------------------------------------

@pytest.mark.slow
@pytest.mark.timeout(180)
def test_burst_of_inserts_all_reach_person(api, unique_surname):
    n = 100

    def insert(i):
        tenant_id = TENANTS[i % len(TENANTS)]
        return tenant_id, api.create_legacy(tenant_id, legacy_payload(tenant_id, f"{unique_surname}-{i}", f"v{i}"))

    with ThreadPoolExecutor(max_workers=10) as pool:
        created = list(pool.map(insert, range(n)))

    def missing():
        have = {(p["tenant_id"], p["legacy_id"]) for p in api.get_persons()}
        return [c for c in created if c not in have]

    deadline = time.monotonic() + 120
    while missing() and time.monotonic() < deadline:
        time.sleep(2)
    assert missing() == [], "rows staged but never transformed into person"


# --- 4. duplicate keys in one batch ----------------------------------------------------------------------------

# These used to wedge the pipeline (issue #16: a dbt batch with the same key twice failed and was retried forever; fixed by
# keeping only the latest event per key in the model). They stay isolated (`poisons_stack`) so a regression cannot take
# the shared stack down; scripts/test-e2e.sh runs each one on its own fresh stack.

@pytest.mark.poisons_stack
def test_insert_then_update_in_one_transaction_ends_with_update(api, unique_surname):
    """Create and update events for one key land in a single dbt batch."""
    conn = connect_legacy("tenant_a1")
    with conn, conn.cursor() as cur:
        cur.execute("INSERT INTO person (firstname, surname, city) VALUES ('Before', %s, 'X') RETURNING id", (unique_surname,))
        legacy_id = cur.fetchone()[0]
        cur.execute("UPDATE person SET firstname = 'After' WHERE id = %s", (legacy_id,))
    conn.close()

    person = wait_for_person(api, "tenant_a1", legacy_id, lambda p: p["firstname"] == "After")
    assert person["firstname"] == "After"


@pytest.mark.poisons_stack
def test_insert_then_quick_update_via_api_ends_with_update(api, unique_surname):
    """Same, but as two separate commits ~tens of ms apart, as the API would produce them."""
    legacy_id = api.create_legacy("tenant_a1", legacy_payload("tenant_a1", unique_surname, "first"))
    r = api.update_legacy("tenant_a1", legacy_id, {"firstname": "Second", "surname": unique_surname, "data_2": "second"})
    assert r.status_code == 200

    person = wait_for_person(api, "tenant_a1", legacy_id, lambda p: p["firstname"] == "Second")
    assert person["extra_field"] == "second"


# --- sync lag --------------------------------------------------------------------------------------------------

def test_sync_lag_under_15_seconds(api, unique_surname, record_property):
    start = time.monotonic()
    legacy_id = api.create_legacy("tenant_b2", legacy_payload("tenant_b2", unique_surname, "lag"))
    wait_for_person(api, "tenant_b2", legacy_id, timeout=60)
    lag = time.monotonic() - start
    record_property("sync_lag_seconds", round(lag, 2))
    print(f"\nsync lag: {lag:.2f}s")
    assert lag < 15
