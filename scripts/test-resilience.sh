#!/usr/bin/env bash
# Resilience scenarios: break part of the stack mid-flight and check the pipeline copes. Entirely inside Docker.
# The test container never touches Docker. Each scenario in tests/e2e/test_resilience.py is split into phases
# (test_<scenario>__arrange / __during / __verify); this script runs a phase in the e2e-test container and does the
# stopping/starting of containers between phases. Phases hand data to each other via test-results/resilience-state.json.
# A scenario whose arrange or during phase fails still gets its containers restarted, and the verify phase still runs.
# Reports: test-results/junit-resilience-<scenario>-<phase>.xml and summary-resilience-<scenario>-<phase>.json,
# plus stack-resilience.log (docker compose ps + all container logs).
# Set KEEP_STACK=1 to leave the stack running. Exit code 0 only if every phase passes.
set -uo pipefail
source "$(dirname "$0")/e2e-lib.sh"

STOPPED=()
cleanup() {
  [ ${#STOPPED[@]} -gt 0 ] && $COMPOSE start "${STOPPED[@]}" >/dev/null 2>&1
  stack_down resilience
}
trap cleanup EXIT

status=0

# phase <scenario> <phase>: run one phase of one scenario in the test container.
phase() {
  echo "--- $1 :: $2 ---"
  local skip_ready=""
  [ "$2" != arrange ] && skip_ready=1  # only arrange runs on a healthy stack; later phases deal with the damage themselves
  $COMPOSE run --rm -e TEST_SUITE="resilience-$1-$2" -e E2E_SKIP_READY="$skip_ready" e2e-test \
    pytest "tests/e2e/test_resilience.py::test_$1__$2" -m destructive -v -p no:cacheprovider \
    --junitxml="/results/junit-resilience-$1-$2.xml" || status=1
}

# hold <services...>: stop services; they are restarted by release (or by the exit trap if we die in between).
hold() { STOPPED=("$@"); $COMPOSE stop "$@" || status=1; }
release() { $COMPOSE start "${STOPPED[@]}" || status=1; STOPPED=(); }

prepare_results
rm -f test-results/resilience-state.json
preflight || exit 1
stack_up || exit 1

# Wait for the pipeline itself (API, connectors) before the first scenario.
$COMPOSE run --rm -e TEST_SUITE=resilience-ready e2e-test \
  pytest tests/e2e/test_resilience.py::test_pipeline_ready -m destructive -q -p no:cacheprovider || { echo "Stack never became ready" >&2; exit 1; }

# 1. cdc-consumer stopped while rows are written to legacy; they must arrive after it restarts.
phase consumer_restart arrange
hold cdc-consumer;              phase consumer_restart during
release;                        phase consumer_restart verify

# 2. A legacy DB is down: writes through the API must fail cleanly, and the pipeline must recover afterwards.
phase legacy_down arrange
hold postgres-legacy-a1;        phase legacy_down during
release;                        phase legacy_down verify

# 3. The new-system DB is down while a legacy row is written; the row must arrive without a manual consumer restart.
#    `recover` restarts the consumer anyway (in case verify failed) and checks the stack works again.
phase new_system_down arrange
hold postgres-new-system;       phase new_system_down during
release;                        phase new_system_down verify
$COMPOSE restart cdc-consumer || status=1
phase new_system_down recover

# 4. Replaying every Kafka event (offset reset) must not change person. The group must be inactive to be reset.
phase offset_replay arrange
hold cdc-consumer
$COMPOSE exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 --group cdc-consumer-group \
  --reset-offsets --to-earliest --all-topics --execute || status=1
release;                        phase offset_replay verify

echo "Reports: test-results/junit-resilience-*.xml, test-results/summary-resilience-*.json"
exit $status
