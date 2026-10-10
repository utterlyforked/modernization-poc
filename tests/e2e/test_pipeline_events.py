"""E2E tests for the pipeline activity feed (issue #15): /api/events and /api/pipeline/status."""
import json

import pytest
import requests

from conftest import API_URL, CONNECTORS, connect_legacy, wait_for_person, wait_until

pytestmark = pytest.mark.e2e


def get_events(**params):
    r = requests.get(f"{API_URL}/api/events", params={"limit": 1000, **params}, timeout=10)
    r.raise_for_status()
    return r.json()


def record_stages(tenant_id, legacy_id):
    """Stages of the events for one record, oldest first."""
    return [e["stage"] for e in get_events(tenant=tenant_id) if e["legacy_id"] == legacy_id]


def create_via_api(api, tenant_id, surname):
    return api.create_person({"tenant_id": tenant_id, "firstname": "Evt", "surname": surname, "city": "Eventville",
                              "date_of_birth": "1990-01-01", "extra_field": "x"}).json()["legacy_id"]


@pytest.mark.parametrize("tenant_id", ["tenant_a1", "tenant_b2"])
def test_created_record_produces_the_expected_stage_sequence(api, unique_surname, tenant_id):
    legacy_id = create_via_api(api, tenant_id, unique_surname)
    wait_for_person(api, tenant_id, legacy_id)

    def journey_complete():
        events = [e for e in get_events(tenant=tenant_id) if e["legacy_id"] == legacy_id]
        return events if {"api", "consumer.received", "consumer.staged"} <= {e["stage"] for e in events} else None

    events = wait_until(journey_complete, desc=f"api/consumer events for {tenant_id} person#{legacy_id}")
    # the first occurrence of each stage is in pipeline order (ids are assigned in write order)
    first = {}
    for e in events:
        first.setdefault(e["stage"], e["id"])
    assert first["api"] < first["consumer.received"] < first["consumer.staged"]

    # dbt events are per batch (no record id): a finished run must follow the staged event
    wait_until(lambda: any(e["stage"] == "dbt" and e["id"] > first["consumer.staged"] and "finished" in e["message"]
                           for e in get_events()), desc="a dbt 'finished' event after the staged event")


def test_events_since_id_returns_only_newer_events(api, unique_surname):
    newest = get_events(limit=1)[-1]["id"]
    legacy_id = create_via_api(api, "tenant_a2", unique_surname)
    wait_until(lambda: any(e["legacy_id"] == legacy_id for e in get_events(since_id=newest)),
               desc="new events after since_id")
    assert all(e["id"] > newest for e in get_events(since_id=newest))


def test_events_filters_by_severity_and_tenant(api):
    api.create_person({"tenant_id": "tenant_b1", "firstname": "NoSurname"})  # 400 -> warn event
    warns = wait_until(lambda: get_events(severity="warn", tenant="tenant_b1"), desc="a warn event for tenant_b1")
    assert {e["severity"] for e in warns} == {"warn"}
    assert {e["tenant_id"] for e in warns} == {"tenant_b1"}
    assert all(e["severity"] in ("warn", "error") for e in get_events(severity="warn,error"))


@pytest.mark.parametrize("tenant_id", ["tenant_a1", "tenant_b1"])
def test_dead_lettered_delete_appears_as_an_error_event(api, unique_surname, tenant_id):
    """A DELETE cannot be staged yet (issue #4), so it is dead-lettered: that must be visible as an error."""
    legacy_id = create_via_api(api, tenant_id, unique_surname)
    wait_for_person(api, tenant_id, legacy_id)

    conn = connect_legacy(tenant_id)
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM person WHERE id = %s", (legacy_id,))
    conn.close()

    def deleted_id(event):
        # the delete's `before` image carries only the primary key (default replica identity)
        try:
            payload = json.loads(event["detail"]["raw_value"])
            return (payload.get("payload", payload).get("before") or {}).get("id")
        except (ValueError, KeyError, TypeError, AttributeError):
            return None

    def dead_lettered():
        return [e for e in get_events(severity="error", tenant=tenant_id)
                if e["stage"] == "consumer.dead_letter" and deleted_id(e) == legacy_id]

    event = wait_until(dead_lettered, desc="consumer.dead_letter error event for the deleted row")[0]
    assert event["detail"]["error"]
    assert event["detail"]["topic"].endswith(".public.person")


def test_pipeline_status_reports_connectors_and_counts():
    r = requests.get(f"{API_URL}/api/pipeline/status", timeout=10)
    r.raise_for_status()
    status = r.json()
    assert status["connectors"] == {name: "RUNNING" for name in CONNECTORS}
    assert isinstance(status["staging_backlog"], int)
    assert isinstance(status["errors"], int)
    assert status["last_consumer_activity"]
