"""Resilience e2e tests: break part of the stack mid-flight and check the pipeline copes.

Run only through `make test-resilience` (scripts/test-resilience.sh). The test container has no Docker access, so each
scenario is split into phases that the script runs in order, stopping/starting containers between them:

    test_<scenario>__arrange   (stack healthy)  ->  [script stops a container]  ->  test_<scenario>__during
    ->  [script starts it again]  ->  test_<scenario>__verify

Phases pass data (legacy ids, snapshots) through a JSON state file in the results volume. All are marked
`destructive`, so the normal e2e run (`-m "not destructive"`) skips them.
"""
import json
import os
import time

import pytest
import requests

from conftest import (CONNECTORS, DEBEZIUM_URL, SETTLE_TIMEOUT, connectors_running, legacy_payload,
                      wait_for_person, wait_until)

pytestmark = [pytest.mark.e2e, pytest.mark.destructive, pytest.mark.slow, pytest.mark.timeout(300)]

STATE_FILE = os.path.join(os.environ.get("TEST_RESULTS_DIR", "test-results"), "resilience-state.json")
CONSUMER_STARTUP = 60  # the consumer sleeps 30s at start-up, then has to join its group


@pytest.fixture
def state(request):
    """Per-scenario dict persisted across phases (scenario = test name between `test_` and `__`)."""
    scenario = request.node.name.split("__")[0].removeprefix("test_")
    data = json.load(open(STATE_FILE)) if os.path.exists(STATE_FILE) else {}
    scenario_state = data.setdefault(scenario, {})
    yield scenario_state
    json.dump(data, open(STATE_FILE, "w"))


def canary(api, tenant_id, surname, timeout=SETTLE_TIMEOUT):
    """Insert a fresh legacy row and wait for it to reach person: proves the pipeline is flowing end to end."""
    legacy_id = api.create_legacy(tenant_id, legacy_payload(tenant_id, surname, "canary"))
    wait_for_person(api, tenant_id, legacy_id, timeout=timeout)
    return legacy_id


def restart_failed_connectors():
    for name in CONNECTORS:
        status = requests.get(f"{DEBEZIUM_URL}/connectors/{name}/status", timeout=5).json()
        states = [status["connector"]["state"]] + [t["state"] for t in status["tasks"]]
        if not status["tasks"] or any(s != "RUNNING" for s in states):
            requests.post(f"{DEBEZIUM_URL}/connectors/{name}/restart?includeTasks=true&onlyFailed=false", timeout=10)
    wait_until(connectors_running, SETTLE_TIMEOUT, desc="all Debezium connectors RUNNING")


def person_snapshot(api):
    """Comparable view of person, ignoring canary rows (they are added by the phases themselves)."""
    return sorted([p["tenant_id"], p["legacy_id"], p["firstname"], p["surname"], p["extra_field"]]
                  for p in api.get_persons() if not p["surname"].endswith("-canary"))


def test_pipeline_ready(api):
    """Runs first (stack_ready fixture does the waiting) and proves rows flow end to end before any scenario."""
    canary(api, "tenant_a1", "ready-canary")


# --- 1. consumer restart -----------------------------------------------------------------------------------------
# Script stops cdc-consumer between arrange and during, starts it between during and verify.

def test_consumer_restart__arrange(api, unique_surname):
    canary(api, "tenant_b1", f"{unique_surname}-canary")


def test_consumer_restart__during(api, unique_surname, state):
    ids = {t: api.create_legacy(t, legacy_payload(t, f"{unique_surname}-{t}", "downtime")) for t in ("tenant_a1", "tenant_b1")}
    time.sleep(3)
    for t, legacy_id in ids.items():
        assert api.find_person(t, legacy_id) is None  # consumer really is the only way in
    state["ids"] = ids


def test_consumer_restart__verify(api, state):
    for t, legacy_id in state["ids"].items():
        wait_for_person(api, t, legacy_id, timeout=CONSUMER_STARTUP + 60)


# --- 2. legacy DB down ---------------------------------------------------------------------------------------------
# Script stops postgres-legacy-a1 for `during`.

def test_legacy_down__arrange(api, unique_surname):
    canary(api, "tenant_a1", f"{unique_surname}-canary")


def test_legacy_down__during(api, unique_surname, state):
    surname = f"{unique_surname}-down"
    state["surname"] = surname
    r = api.create_person({"tenant_id": "tenant_a1", "firstname": "Down", "surname": surname})
    assert not r.ok, f"POST succeeded with the legacy DB down: {r.status_code}"
    assert requests.get(f"{api.base_url}/api/health", timeout=5).status_code == 200  # NB: health never touches a DB


def test_legacy_down__verify(api, unique_surname, state):
    wait_until(lambda: requests.get(f"{api.base_url}/api/legacy/tenant_a1/persons", timeout=5).ok,
               SETTLE_TIMEOUT, desc="legacy a1 readable through the API again")
    restart_failed_connectors()

    # The canary doubles as a barrier: once it arrives, a stray row from the failed POST would have too.
    canary(api, "tenant_a1", f"{unique_surname}-canary")
    assert [p for p in api.get_persons() if p["surname"] == state["surname"]] == []
    legacy_rows = requests.get(f"{api.base_url}/api/legacy/tenant_a1/persons", timeout=10).json()
    assert [p for p in legacy_rows if p["surname"] == state["surname"]] == []


# --- 3. new-system DB down while legacy is written -----------------------------------------------------------------
# Script stops postgres-new-system for `during`. After `verify` it restarts cdc-consumer and runs `recover`.

def test_new_system_down__arrange(api, unique_surname):
    canary(api, "tenant_b2", f"{unique_surname}-canary")


def test_new_system_down__during(api, unique_surname, state):
    state["id"] = api.create_legacy("tenant_b2", legacy_payload("tenant_b2", unique_surname, "while-down"))
    time.sleep(10)  # give the consumer time to try to stage it and fail


@pytest.mark.xfail(strict=True, reason="issue #4: after the new-system DB restarts the consumer keeps its dead connection "
                   "('connection already closed' / 'server closed the connection unexpectedly') and stages nothing "
                   "until it is restarted by hand (stack-resilience.log; the row arrives after the restart)")
def test_new_system_down__verify(api, state):
    """Pass => consumer reconnects by itself. Fail => it holds a dead connection and needs a manual restart."""
    wait_until(lambda: requests.get(f"{api.base_url}/api/persons", timeout=5).ok, SETTLE_TIMEOUT,
               desc="API reading the new system again")
    wait_for_person(api, "tenant_b2", state["id"], timeout=90)


def test_new_system_down__recover(api, unique_surname, state):
    """After the script restarted the consumer: the missed row must arrive (offset not lost) and the pipeline flows."""
    wait_for_person(api, "tenant_b2", state["id"], timeout=CONSUMER_STARTUP + 60)
    canary(api, "tenant_b2", f"{unique_surname}-canary", timeout=CONSUMER_STARTUP + 60)


# --- 4. offset replay ----------------------------------------------------------------------------------------------
# Script stops cdc-consumer, resets group cdc-consumer-group to earliest, starts it again before `verify`.

def test_offset_replay__arrange(api, unique_surname, state):
    for t in ("tenant_a2", "tenant_b1"):
        wait_for_person(api, t, api.create_legacy(t, legacy_payload(t, f"{unique_surname}-{t}", "replay")))
    state["before"] = person_snapshot(api)


def test_offset_replay__verify(api, unique_surname, state):
    canary(api, "tenant_a2", f"{unique_surname}-canary", timeout=CONSUMER_STARTUP + 120)  # pipeline survived the replay
    time.sleep(5)
    assert person_snapshot(api) == state["before"]
