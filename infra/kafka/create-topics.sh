#!/usr/bin/env bash
# Create every topic the stack uses. The broker has auto.create.topics.enable=false,
# so this runs once per `docker compose up` (as kafka-init). Re-running it is safe:
# topics that already exist are left as they are.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:19092}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

create() {
  local topic=$1 partitions=$2
  shift 2
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 \
    --config compression.type=zstd "$@"
}

for ns in live eval bench; do
  create "$ns.events" 8 --config retention.ms=86400000
  create "$ns.aggregates" 4
  create "$ns.scores" 4
  create "$ns.alerts" 1
  create "$ns.unit-status" 1 --config cleanup.policy=compact
done

echo "topics:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
