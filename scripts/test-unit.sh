#!/usr/bin/env bash
# Runs the unit suite (tests/unit) entirely inside Docker. No stack, network or services needed.
# Reports: test-results/junit-unit.xml and test-results/summary-unit.json. Exit code 0 only if all tests pass.
set -uo pipefail
cd "$(dirname "$0")/.."

HOST_UID=${SUDO_UID:-$(id -u)} HOST_GID=${SUDO_GID:-$(id -g)}  # under sudo, own test-results as the invoking user
mkdir -p test-results
[ "$(id -u)" = 0 ] && chown "$HOST_UID:$HOST_GID" test-results

docker build -q -f Dockerfile.unit-test -t modernization-poc-unit-test . >/dev/null || exit 1
docker run --rm --network none --user "$HOST_UID:$HOST_GID" \
  -v "$PWD/test-results:/results" modernization-poc-unit-test
status=$?
echo "Reports: test-results/junit-unit.xml, test-results/summary-unit.json"
exit $status
