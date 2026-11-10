#!/usr/bin/env bash
# Start the demo on an empty stack; `make demo` runs it after `make jar up`. The generator
# writes three weeks of synthetic history at full speed and then continues live. The job is
# submitted once every partition of live.events has data, and the script waits until the job
# has caught up with the wall clock.
#
# The timeline starts three weeks before the generator starts, so it cannot restart on a stack
# that already holds events: the new history would be older than the job's watermark and state.
# The script then refuses and names `make reset`.
set -euo pipefail

cd "$(dirname "$0")/.."

TOPIC=live.events
PARTITIONS_TIMEOUT_S=120
WARMUP_TIMEOUT_S=900
POLL_S=5
# Caught up when the newest closed minute starts at most this long before now.
CAUGHT_UP_S=120

# Local demo credential, not a secret.
ch() {
  curl -sf --user tb:tb-local --data-binary "$1" http://localhost:8123/
}

# End offsets of the events topic, one "topic:partition:offset" line per partition. The CLI
# tool runs in the broker's container, so it gets a small heap of its own.
end_offsets() {
  docker compose exec -T -e KAFKA_HEAP_OPTS=-Xmx128m kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC"
}

# "<partitions with data> <partitions>" for the events topic.
partitions_with_data() {
  end_offsets | awk -F: 'NF == 3 { n++; if ($3 > 0) d++ } END { print d + 0, n + 0 }'
}

all_partitions_have_data() {
  local with_data total
  read -r with_data total < <(partitions_with_data)
  [ "$total" -gt 0 ] && [ "$with_data" -eq "$total" ]
}

read -r with_data total < <(partitions_with_data)
if [ "$total" -eq 0 ]; then
  echo "could not read the end offsets of $TOPIC" >&2
  exit 1
fi
if [ "$with_data" -gt 0 ]; then
  echo 'live.events already has data. The demo timeline cannot restart in place: run "make reset" first.'
  exit 1
fi

echo "Starting the demo: three weeks of synthetic history, then live traffic." \
  "Warm-up takes about 3.5 minutes."

start=$SECONDS
docker compose --profile demo up -d --build --no-deps generator

# The job must not start before every partition has data: the live watermark leaves out a split
# that is still empty after 60 s, and that split's history would then arrive behind the
# watermark and be dropped as late.
deadline=$((SECONDS + PARTITIONS_TIMEOUT_S))
until all_partitions_have_data; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "not every partition of $TOPIC has data after ${PARTITIONS_TIMEOUT_S} s;" \
      "see docker compose --profile demo logs generator" >&2
    exit 1
  fi
  sleep 1
done
docker compose exec -T jobmanager bash /opt/tb-scripts/submit-job.sh

# Prints "<closed minutes> <seconds behind now> <percent of the history> <newest minute>".
progress() {
  ch "SELECT count(), dateDiff('second', max(minute), now()),
      round(100 * dateDiff('second', min(minute), max(minute))
        / greatest(1, dateDiff('second', min(minute), now()))),
      formatDateTime(max(minute), '%Y-%m-%d %H:%i', 'UTC')
    FROM tb.aggregates WHERE ns = 'live' FORMAT TSV"
}

deadline=$((SECONDS + WARMUP_TIMEOUT_S))
while :; do
  if row=$(progress); then
    IFS=$'\t' read -r rows behind_s percent newest <<< "$row"
    if [ "$rows" -gt 0 ] && [ "$behind_s" -le "$CAUGHT_UP_S" ]; then
      break
    fi
    if [ "$rows" -gt 0 ]; then
      echo "warming baselines: ${percent}% (history at $newest UTC, $((SECONDS - start)) s)"
    else
      echo "warming baselines: 0% (no minute closed yet, $((SECONDS - start)) s)"
    fi
  else
    echo "warming baselines: could not query ClickHouse ($((SECONDS - start)) s)"
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "the job has not caught up after $((WARMUP_TIMEOUT_S / 60)) minutes;" \
      "see http://localhost:8081 and docker compose logs taskmanager" >&2
    exit 1
  fi
  sleep "$POLL_S"
done

echo "caught up after $((SECONDS - start)) s"
echo
echo "Grafana http://localhost:3000/d/tenant-baselines"
echo "Alertmanager http://localhost:9093"
echo "Flink http://localhost:8081"
echo
script_line=$(docker compose --profile demo logs --no-log-prefix generator 2> /dev/null \
  | grep -m 1 'demo script:' || true)
echo "generator: ${script_line:-no demo script line in its log yet}"
echo
echo "What to watch, counted from the generator's catch-up (the UTC times above):"
echo "  the tenant drop starts about 2 minutes after catch-up and pages after about 5 to 8 more;"
echo "  the regional error spike starts about 14 minutes after catch-up and pages about 4 minutes later;"
echo "  alerts decided during the history appear in the alert log but are never paged."
