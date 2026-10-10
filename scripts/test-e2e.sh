#!/usr/bin/env bash
# Runs the e2e suite (everything not marked `destructive`) against the full compose stack, entirely inside Docker.
# Brings the stack up, runs tests/e2e in a container on the compose network, then tears the stack
# down (including volumes) so every run starts fresh. Set KEEP_STACK=1 to leave it running.
# E2E_RUNS=2 repeats the suite against the same stack (re-runnability check).
# Tests marked `poisons_stack` then run one by one, each on a fresh stack.
# Reports: test-results/junit-e2e.xml and test-results/summary-e2e.json,
# plus test-results/e2e-run.log (test output) and test-results/stack-e2e.log (docker compose ps + all container logs). Exit code 0 only if all runs pass.
# Container-stopping scenarios live in scripts/test-resilience.sh.
set -uo pipefail
source "$(dirname "$0")/e2e-lib.sh"

prepare_results
rm -f test-results/e2e-run.log
preflight || exit 1
stack_up || { stack_down e2e; exit 1; }

status=0
for i in $(seq 1 "${E2E_RUNS:-1}"); do
  echo "=== e2e run $i/${E2E_RUNS:-1} ==="
  run_logged e2e-run.log $COMPOSE run -T --rm --build e2e-test || status=$?
done

stack_down e2e

# `poisons_stack` tests can wedge the pipeline for good, so each one gets a fresh stack of its own.
# Reports: junit-e2e-isolated-<n>.xml / summary-e2e-isolated-<n>.json, log in e2e-run.log, stack-e2e-isolated-<n>.log.
mapfile -t isolated < <($COMPOSE run -T --rm --no-deps -e TEST_SUITE=e2e-collect e2e-test pytest tests/e2e -m "poisons_stack and not destructive" \
  --collect-only -q -p no:cacheprovider 2>/dev/null | grep '^tests/')
n=0
for test_id in "${isolated[@]}"; do
  n=$((n + 1))
  echo "=== isolated test $n/${#isolated[@]}: $test_id ==="
  stack_up || { stack_down "e2e-isolated-$n"; status=1; continue; }
  run_logged e2e-run.log $COMPOSE run -T --rm -e TEST_SUITE="e2e-isolated-$n" e2e-test \
    pytest "$test_id" -v -p no:cacheprovider --junitxml="/results/junit-e2e-isolated-$n.xml" || status=$?
  stack_down "e2e-isolated-$n"
done

echo "Reports: test-results/junit-e2e*.xml, test-results/summary-e2e*.json, test-results/e2e-run.log, test-results/stack-e2e*.log"
exit $status
