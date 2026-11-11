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
  # The job's sink stamps records with event time, and a replay's event time is months old, so
  # retention would delete eval and bench output before it is read. Append time avoids that.
  stamp=()
  if [ "$ns" != live ]; then
    stamp=(--config message.timestamp.type=LogAppendTime)
  fi
  create "$ns.events" 8 --config retention.ms=86400000
  create "$ns.aggregates" 4 ${stamp[@]+"${stamp[@]}"}
  create "$ns.scores" 4 ${stamp[@]+"${stamp[@]}"}
  create "$ns.alerts" 1 ${stamp[@]+"${stamp[@]}"}
  create "$ns.unit-status" 1 --config cleanup.policy=compact ${stamp[@]+"${stamp[@]}"}
done

echo "topics:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
