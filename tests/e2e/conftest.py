"""E2E fixtures. Run against a running stack: `docker compose up -d --wait`, then `pytest tests/e2e`."""
import os
import time
import uuid

import pytest
import requests

API_URL = os.environ.get("API_URL", "http://localhost:5000")
DEBEZIUM_URL = os.environ.get("DEBEZIUM_URL", "http://localhost:8083")
CONNECTORS = [f"legacy-{t}-connector" for t in ("a1", "a2", "b1", "b2")]
TENANTS = ["tenant_a1", "tenant_a2", "tenant_b1", "tenant_b2"]
SETTLE_TIMEOUT = 180  # stack start-up (consumer sleeps 30s, api 10s)
SYNC_TIMEOUT = 30


def pytest_collection_modifyitems(items):
    for item in items:
        if "e2e" in item.nodeid.split("/"):
            item.add_marker(pytest.mark.e2e)


def wait_until(fn, timeout=SYNC_TIMEOUT, interval=0.5, desc="condition"):
    """Poll fn() until it returns a truthy value and return it; fail on timeout."""
    deadline = time.monotonic() + timeout
    last_error = None
    while True:
        try:
            result = fn()
            if result:
                return result
        except requests.RequestException as e:
            last_error = e
        if time.monotonic() >= deadline:
            raise AssertionError(f"Timed out after {timeout}s waiting for {desc}"
                                 + (f" (last error: {last_error})" if last_error else ""))
        time.sleep(interval)


class Api:
    def __init__(self, base_url):
        self.base_url = base_url

    def get_persons(self):
        r = requests.get(f"{self.base_url}/api/persons", timeout=10)
        r.raise_for_status()
        return r.json()

    def find_person(self, tenant_id, legacy_id):
        for p in self.get_persons():
            if p["tenant_id"] == tenant_id and p["legacy_id"] == int(legacy_id):
                return p
        return None

    def create_person(self, payload):
        return requests.post(f"{self.base_url}/api/persons", json=payload, timeout=10)

    def update_person(self, payload):
        return requests.put(f"{self.base_url}/api/persons", json=payload, timeout=10)

    def create_legacy(self, tenant_id, payload):
        r = requests.post(f"{self.base_url}/api/legacy/{tenant_id}/persons", json=payload, timeout=10)
        r.raise_for_status()
        return r.json()["legacy_id"]

    def get_legacy(self, tenant_id, legacy_id):
        r = requests.get(f"{self.base_url}/api/legacy/{tenant_id}/persons/{legacy_id}", timeout=10)
        r.raise_for_status()
        return r.json()


@pytest.fixture(scope="session", autouse=True)
def stack_ready():
    """Wait for the API and all Debezium connectors before any e2e test runs."""
    def api_healthy():
        return requests.get(f"{API_URL}/api/health", timeout=5).status_code == 200

    def connectors_running():
        for name in CONNECTORS:
            r = requests.get(f"{DEBEZIUM_URL}/connectors/{name}/status", timeout=5)
            if r.status_code != 200:
                return False
            status = r.json()
            if status["connector"]["state"] != "RUNNING":
                return False
            if not status["tasks"] or any(t["state"] != "RUNNING" for t in status["tasks"]):
                return False
        return True

    wait_until(api_healthy, SETTLE_TIMEOUT, desc="API /api/health")
    wait_until(connectors_running, SETTLE_TIMEOUT, desc="all Debezium connectors RUNNING")


@pytest.fixture(scope="session")
def api():
    return Api(API_URL)


@pytest.fixture
def unique_surname():
    return f"E2E-{uuid.uuid4().hex[:12]}"
