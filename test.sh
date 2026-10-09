#!/usr/bin/env bash
# Runs the test suite entirely inside Docker. Only Docker is needed on the host.
# Reports: test-results/junit.xml (JUnit XML) and test-results/summary.json.
# Exit code: 0 if all tests passed, non-zero otherwise.
set -uo pipefail
cd "$(dirname "$0")"

export HOST_UID=$(id -u) HOST_GID=$(id -g)
COMPOSE="docker compose -f docker-compose.dbt-test.yml"

rm -rf test-results && mkdir -p test-results
$COMPOSE up --build --abort-on-container-exit --exit-code-from dbt-test
status=$?

$COMPOSE down -v --remove-orphans >/dev/null 2>&1
echo "Reports: test-results/junit.xml, test-results/summary.json"
exit $status
