#!/usr/bin/env bash
# Runs the e2e suite (everything not marked `destructive`) against the full compose stack, entirely inside Docker.
# Brings the stack up, runs tests/e2e in a container on the compose network, then tears the stack
# down (including volumes) so every run starts fresh. Set KEEP_STACK=1 to leave it running.
# E2E_RUNS=2 repeats the suite against the same stack (re-runnability check).
# Reports: test-results/junit-e2e.xml and test-results/summary-e2e.json. Exit code 0 only if all runs pass.
# Container-stopping scenarios live in scripts/test-resilience.sh.
set -uo pipefail
source "$(dirname "$0")/e2e-lib.sh"

prepare_results
preflight || exit 1
stack_up || { stack_down; exit 1; }

status=0
for i in $(seq 1 "${E2E_RUNS:-1}"); do
  echo "=== e2e run $i/${E2E_RUNS:-1} ==="
  $COMPOSE run --rm --build e2e-test || status=$?
done

stack_down
echo "Reports: test-results/junit-e2e.xml, test-results/summary-e2e.json"
exit $status
