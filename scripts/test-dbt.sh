#!/usr/bin/env bash
# Runs the test suite entirely inside Docker. Only Docker is needed on the host.
# Reports: test-results/junit-dbt.xml (JUnit XML) and test-results/summary-dbt.json.
# Exit code: 0 if all tests passed, non-zero otherwise.
set -uo pipefail
cd "$(dirname "$0")/.."

export HOST_UID=${SUDO_UID:-$(id -u)} HOST_GID=${SUDO_GID:-$(id -g)}  # under sudo, own test-results as the invoking user
COMPOSE="docker compose -f docker-compose.dbt-test.yml"

mkdir -p test-results && rm -f test-results/junit-dbt.xml test-results/summary-dbt.json
[ "$(id -u)" = 0 ] && chown "$HOST_UID:$HOST_GID" test-results
$COMPOSE up --build --abort-on-container-exit --exit-code-from dbt-test
status=$?

$COMPOSE down -v --remove-orphans >/dev/null 2>&1
echo "Reports: test-results/junit-dbt.xml, test-results/summary-dbt.json"
exit $status
