# modernization-poc

POC of a legacy-to-modern migration via CDC. Four legacy Postgres DBs (tenants `tenant_a1`, `tenant_a2` = Legacy A with
`data_1/2/3`; `tenant_b1`, `tenant_b2` = Legacy B with `data_a/b/c`) -> Debezium -> Kafka topics `legacy_<x>.public.person`
-> `cdc-consumer/consumer.py` -> `person_staging` (new-system Postgres) -> `dbt run` (`dbt-project/models/person_unified.sql`,
incremental, alias `person`) -> `person`. `data_2` (A) and `data_c` (B) both map to `extra_field`; `modernized_only` exists
only in the new system. The Flask API (`api/api.py`, :5000) reads `person` and writes through to the legacy DBs; changes come
back via CDC. Web UI on :8000, Debezium REST on :8083. README.md has the full architecture; docs/TESTING.md is a manual runbook.

## Hard rules
- **Everything runs in Docker; nothing on the host.** No venvs, no `pip install`, no running pytest/dbt on the host. Only
  Docker, compose and make are host dependencies, so the POC stays portable.
- **Never give a container Docker access** (no `/var/run/docker.sock` mount, no docker CLI in an image). That is root on the
  host. Container stop/start belongs in host-side scripts (`scripts/test-resilience.sh`), not in test code.
- **I (Claude) cannot reach the Docker socket** (user is not in the `docker` group). Don't work around it. When a run is
  needed, give the user the exact `make` command (they run it with sudo) and ask them to paste the output. Don't claim
  anything passed that wasn't run. `make -n` is not safe for `test*`: recursive `$(MAKE)` lines still execute.

## Commands (all via make; `make help` lists them)
| Command | Purpose |
|---|---|
| `make up` / `down` / `clean` / `restart` | Start (waits until ready) / stop / stop + delete volumes. Containers have fixed names. |
| `make logs SERVICE=cdc-consumer`, `make ps`, `make debug` | Inspect the running stack |
| `make test-unit` | Unit tests (`tests/unit`), no network, no stack |
| `make test-dbt` | dbt model tests (`tests/dbt`) against a throwaway Postgres |
| `make test-e2e` | `tests/e2e` minus `destructive`; brings up its own stack (project `modernization-poc-e2e`) and tears it down |
| `make test-resilience` | Phased container-stopping scenarios (`tests/e2e/test_resilience.py`), driven by `scripts/test-resilience.sh` |
| `make test` | All four suites; runs all, fails if any failed |

Run `make down` before `make test-e2e`/`test-resilience` (preflight aborts if a dev stack's containers exist). `KEEP_STACK=1`
keeps the e2e stack up; `E2E_RUNS=2` repeats the e2e suite. Reports: `test-results/junit-<suite>.xml` and
`summary-<suite>.json` (git-ignored).

## Test harness layout
- One Dockerfile per suite (`Dockerfile.unit-test`, `.dbt-test`, `.e2e-test`) plus `docker-compose.dbt-test.yml` /
  `docker-compose.e2e-test.yml`; runners are `scripts/test-*.sh`, e2e ones share `scripts/e2e-lib.sh`.
- `tests/conftest.py` writes the per-suite summary (`TEST_SUITE` env). `pytest.ini` has the markers (`e2e`, `slow`,
  `destructive`) and a 60s default timeout.
- e2e helpers live in `tests/e2e/conftest.py` (`wait_until`, `wait_for_person`, `Api`, direct DB connections on the compose
  network). Tests use unique surnames and never assume an empty DB; they only poll, never sleep for correctness.
- Resilience scenarios are phases `test_<scenario>__arrange|during|verify`; the script stops/starts containers between them
  and passes state via `test-results/resilience-state.json`. A scenario's `during`/`verify` phases set `E2E_SKIP_READY=1`.

## Gotchas
- API dates serialise as RFC 822 (`Wed, 01 May 2024 ...`); use `as_date()` in tests.
- Consumer sleeps 30s at start-up and runs dbt at most once a second when idle; settle timeouts are generous for that.
- Known/suspected pipeline bugs (issue #4): DELETE events lost (`after` is null in `stage_record`), consumer swallows DB
  errors then commits offsets, no reconnect after the new-system DB restarts, duplicate keys in one dbt batch can break the
  incremental model (and stall all later runs). Confirmed ones are `xfail(strict=True)` with reasons.

## Workflow
Issues are worked on branches `issue-<N>-<slug>` from `main`; commit messages end with `(#N)`; PRs say `Closes #N`. Don't
push, open PRs, comment or close issues without asking.
