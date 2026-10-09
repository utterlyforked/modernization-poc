import json
import os
import subprocess

import psycopg2
import pytest

DBT_DIR = "/dbt"
UNIQUE_CONSTRAINT = "person_tenant_id_legacy_id_key"


@pytest.fixture
def db():
    conn = psycopg2.connect(
        host=os.environ["NEW_SYSTEM_DB_HOST"],
        port=os.environ["NEW_SYSTEM_DB_PORT"],
        dbname=os.environ["NEW_SYSTEM_DB_NAME"],
        user=os.environ["NEW_SYSTEM_DB_USER"],
        password=os.environ["NEW_SYSTEM_DB_PASS"],
    )
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def seeded(db):
    """Clean tables, restore the unique constraint, seed one staging row per tenant, build the model."""
    with db.cursor() as cur:
        cur.execute("TRUNCATE person, person_staging RESTART IDENTITY")
        cur.execute(f"ALTER TABLE person DROP CONSTRAINT IF EXISTS {UNIQUE_CONSTRAINT}")
        cur.execute(f"ALTER TABLE person ADD CONSTRAINT {UNIQUE_CONSTRAINT} UNIQUE (tenant_id, legacy_id)")
        cur.execute("""
            INSERT INTO person_staging
              (tenant_id, source_id, source_table, operation, firstname, surname, date_of_birth, city, data_2, data_c)
            VALUES
              ('tenant_a1', 1, 'person', 'INSERT', 'Ann',  'Archer', '1990-01-01', 'Leeds', 'x', NULL),
              ('tenant_a2', 1, 'person', 'INSERT', 'Bob',  'Baker',  '1991-02-02', 'York',  'y', NULL),
              ('tenant_b1', 1, 'person', 'INSERT', 'Cara', 'Cole',   '1992-03-03', 'Hull',  NULL, 'z'),
              ('tenant_b2', 1, 'person', 'INSERT', 'Dan',  'Dyer',   '1993-04-04', 'Bath',  NULL, 'w')
        """)
    run = dbt("run")
    assert run.returncode == 0, run.stdout
    return db


def dbt(command):
    return subprocess.run(
        ["dbt", "--no-use-colors", command, "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
        capture_output=True, text=True,
    )


def failed_dbt_tests():
    """Names of dbt tests that failed/errored in the last invocation (from run_results.json)."""
    with open(os.path.join(DBT_DIR, "target", "run_results.json")) as f:
        results = json.load(f)["results"]
    return [r["unique_id"] for r in results if r["status"] in ("fail", "error")]


def test_dbt_tests_pass_on_valid_data(seeded):
    result = dbt("test")
    assert result.returncode == 0, result.stdout
    assert failed_dbt_tests() == []


def test_dbt_fails_on_unknown_tenant(seeded):
    with seeded.cursor() as cur:
        cur.execute("INSERT INTO person (tenant_id, legacy_id, firstname, surname) VALUES ('tenant_bad', 99, 'Eve', 'Evil')")
    assert dbt("test").returncode != 0
    failed = failed_dbt_tests()
    assert any("accepted_values" in t and "tenant_id" in t for t in failed), failed


def test_dbt_fails_on_duplicate_key(seeded):
    # The DB constraint normally prevents this; drop it to prove the dbt test is the safety net.
    with seeded.cursor() as cur:
        cur.execute(f"ALTER TABLE person DROP CONSTRAINT {UNIQUE_CONSTRAINT}")
        cur.execute("INSERT INTO person (tenant_id, legacy_id, firstname, surname) VALUES ('tenant_a1', 1, 'Dup', 'Licate')")
    assert dbt("test").returncode != 0
    failed = failed_dbt_tests()
    assert any("person_unique_tenant_legacy_id" in t for t in failed), failed
