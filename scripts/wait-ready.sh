#!/usr/bin/env bash
# Wait until every service of the compose stack is ready. Each check polls for up to
# TIMEOUT seconds; the script exits 1 and names the first check that failed.
set -euo pipefail

cd "$(dirname "$0")/.."

TIMEOUT="${TIMEOUT:-180}"

container_state() {
  local id
  id=$(docker compose ps -aq "$1")
  [ -n "$id" ] && docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$id"
}

kafka_healthy() {
  local id
  id=$(docker compose ps -q kafka)
  [ -n "$id" ] && [ "$(docker inspect -f '{{.State.Health.Status}}' "$id")" = healthy ]
}

kafka_init_done() {
  local state
  state=$(container_state kafka-init) || return 1
  case "$state" in
    "exited 0") return 0 ;;
    exited*) echo "kafka-init exited with code ${state#exited }" >&2; exit 1 ;;
    *) return 1 ;;
  esac
}

clickhouse_ping() {
  curl -sf http://localhost:8123/ping > /dev/null
}

flink_has_taskmanager() {
  local n
  n=$(curl -sf http://localhost:8081/overview | sed -n 's/.*"taskmanagers":\([0-9]*\).*/\1/p')
  [ -n "$n" ] && [ "$n" -ge 1 ]
}

alertmanager_ready() {
  curl -sf http://localhost:9093/-/ready > /dev/null
}

grafana_healthy() {
  curl -sf http://localhost:3000/api/health > /dev/null
}

wait_for() {
  local name=$1 check=$2
  local deadline=$((SECONDS + TIMEOUT))
  until "$check"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "not ready after ${TIMEOUT}s: $name" >&2
      exit 1
    fi
    sleep 2
  done
  echo "ready $name"
}

wait_for kafka kafka_healthy
wait_for kafka-init kafka_init_done
wait_for clickhouse clickhouse_ping
wait_for flink flink_has_taskmanager
wait_for alertmanager alertmanager_ready
wait_for grafana grafana_healthy
