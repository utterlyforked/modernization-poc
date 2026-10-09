"""E2E fixtures. Run via `make test-e2e` (and `make test-resilience`), which run this inside Docker on the compose network."""
import os
from email.utils import parsedate_to_datetime
import time
import uuid

import psycopg2
import pytest
import requests

API_URL = os.environ.get("API_URL", "http://localhost:5000")
DEBEZIUM_URL = os.environ.get("DEBEZIUM_URL", "http://localhost:8083")
CONNECTORS = [f"legacy-{t}-connector" for t in ("a1", "a2", "b1", "b2")]
TENANTS = ["tenant_a1", "tenant_a2", "tenant_b1", "tenant_b2"]
LEGACY_A = ["tenant_a1", "tenant_a2"]
# Databases are not published to the host; the tests reach them directly on the compose network.
LEGACY_DB_HOST = os.environ.get("LEGACY_DB_HOST_TEMPLATE", "postgres-legacy-{suffix}")  # suffix: a1, a2, b1, b2
NEW_DB_HOST = os.environ.get("NEW_SYSTEM_DB_HOST", "postgres-new-system")
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


def wait_for_person(api, tenant_id, legacy_id, predicate=lambda p: True, timeout=SYNC_TIMEOUT):
    def check():
        person = api.find_person(tenant_id, legacy_id)
        return person if person and predicate(person) else None
    return wait_until(check, timeout=timeout, desc=f"{tenant_id}/{legacy_id} in /api/persons")


def legacy_payload(tenant_id, surname, extra):
    base = {"firstname": "Direct", "surname": surname, "date_of_birth": "1990-01-01", "city": "Legacyville"}
    if tenant_id in LEGACY_A:
        return {**base, "data_1": "one", "data_2": extra, "data_3": "three"}
    return {**base, "data_a": "aaa", "data_b": "bbb", "data_c": extra}


def as_date(value):
    """ISO date string for a date from /api/persons (Flask renders dates as RFC 822), or None."""
    if value is None:
        return None
    try:
        return parsedate_to_datetime(value).date().isoformat()
    except (TypeError, ValueError):
        return value


def connect_legacy(tenant_id):
    """Direct psycopg2 connection to a tenant's legacy DB (the test container is on the compose network)."""
    suffix = tenant_id.split("_")[1]
    return psycopg2.connect(host=LEGACY_DB_HOST.format(suffix=suffix), dbname=f"legacy_{suffix}",
                            user="legacyuser", password="legacypass", connect_timeout=5)


def connect_new_system():
    return psycopg2.connect(host=NEW_DB_HOST, dbname="new_system", user="newuser", password="newpass",
                            connect_timeout=5)


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

    def update_legacy(self, tenant_id, legacy_id, payload):
        return requests.put(f"{self.base_url}/api/legacy/{tenant_id}/persons/{legacy_id}", json=payload, timeout=10)

    def get_legacy(self, tenant_id, legacy_id):
        r = requests.get(f"{self.base_url}/api/legacy/{tenant_id}/persons/{legacy_id}", timeout=10)
        r.raise_for_status()
        return r.json()


@pytest.fixture(scope="session", autouse=True)
def stack_ready():
    """Wait for the API and all Debezium connectors before any e2e test runs.

    Skipped (E2E_SKIP_READY=1) for resilience phases that run while part of the stack is deliberately down.
    """
    if os.environ.get("E2E_SKIP_READY"):
        return

    def api_healthy():
        return requests.get(f"{API_URL}/api/health", timeout=5).status_code == 200

    wait_until(api_healthy, SETTLE_TIMEOUT, desc="API /api/health")
    wait_until(connectors_running, SETTLE_TIMEOUT, desc="all Debezium connectors RUNNING")


@pytest.fixture(scope="session")
def api():
    return Api(API_URL)


@pytest.fixture
def unique_surname():
    return f"E2E-{uuid.uuid4().hex[:12]}"
