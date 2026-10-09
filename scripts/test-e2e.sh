#!/usr/bin/env bash
# Runs the e2e suite against the full compose stack, entirely inside Docker.
# Brings the stack up, runs tests/e2e in a container on the compose network, then tears the stack
# down (including volumes) so every run starts fresh. Set KEEP_STACK=1 to leave it running.
# E2E_RUNS=2 repeats the suite against the same stack (re-runnability check).
# Reports: test-results/junit-e2e.xml and test-results/summary.json. Exit code 0 only if all runs pass.
set -uo pipefail
cd "$(dirname "$0")/.."

export HOST_UID=${SUDO_UID:-$(id -u)} HOST_GID=${SUDO_GID:-$(id -g)}  # under sudo, own test-results as the invoking user
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.e2e-test.yml"

mkdir -p test-results
$COMPOSE up -d --build --remove-orphans $($COMPOSE config --services | grep -v -E '^(e2e-test|debezium-init)$') debezium-init || exit 1

status=0
for i in $(seq 1 "${E2E_RUNS:-1}"); do
  echo "=== e2e run $i/${E2E_RUNS:-1} ==="
  $COMPOSE run --rm --build e2e-test || status=$?
done

if [ -z "${KEEP_STACK:-}" ]; then
  $COMPOSE down -v --remove-orphans >/dev/null 2>&1
fi
echo "Reports: test-results/junit-e2e.xml, test-results/summary.json"
exit $status
