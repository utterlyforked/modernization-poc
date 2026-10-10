# Shared by test-e2e.sh and test-resilience.sh (sourced, not run). Everything runs through Docker on the host;
# the test container never gets Docker access - container control (stop/start) happens only in these scripts.
cd "$(dirname "${BASH_SOURCE[0]}")/.."

export HOST_UID=${SUDO_UID:-$(id -u)} HOST_GID=${SUDO_GID:-$(id -g)}  # under sudo, own test-results as the invoking user
E2E_PROJECT=modernization-poc-e2e  # own project: teardown (down -v) can never touch a dev stack from `make up`
COMPOSE="docker compose -p $E2E_PROJECT -f docker-compose.yml -f docker-compose.e2e-test.yml"

prepare_results() {
  mkdir -p test-results
  [ "$(id -u)" = 0 ] && chown "$HOST_UID:$HOST_GID" test-results
  return 0
}

# Containers have fixed names, so a running dev stack would collide; stop here with a clear message instead.
preflight() {
  local clash
  clash=$(docker ps -a --format '{{.Names}} {{.Label "com.docker.compose.project"}}' |
    awk -v p="$E2E_PROJECT" '$2 != p && $1 ~ /^(zookeeper|kafka|postgres-.*|debezium|debezium-init|cdc-consumer|api|web)$/ {print $1}')
  if [ -n "$clash" ]; then
    echo "Error: containers from another stack are present ($(echo $clash | tr '\n' ' ')). Run 'make down' first." >&2
    return 1
  fi
}

# run_logged <logfile> <command...>: run a command, copy its output to test-results/<logfile>, return its exit code.
run_logged() {
  local log="test-results/$1"; shift
  "$@" 2>&1 | tee -a "$log"
  local rc=${PIPESTATUS[0]}
  [ "$(id -u)" = 0 ] && chown "$HOST_UID:$HOST_GID" "$log"
  return $rc
}

stack_up() {
  $COMPOSE up -d --build --remove-orphans $($COMPOSE config --services | grep -v -E '^(e2e-test|debezium-init)$') debezium-init
}

# stack_down <suite>: save container state and logs to test-results/stack-<suite>.log, then tear the stack down.
stack_down() {
  local log="test-results/stack-${1:-e2e}.log"
  { $COMPOSE ps -a; $COMPOSE logs --no-color --timestamps; } >"$log" 2>&1
  [ "$(id -u)" = 0 ] && chown "$HOST_UID:$HOST_GID" "$log"
  if [ -z "${KEEP_STACK:-}" ]; then
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1
  fi
}
