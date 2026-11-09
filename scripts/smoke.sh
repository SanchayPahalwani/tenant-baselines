#!/usr/bin/env bash
# End-to-end smoke test of the demo; `make smoke` runs it after `make demo`. Checks 1-4 are
# polled every 15 s for up to 12 minutes. Check 5 runs once after them. Check 6 restores the job
# from a checkpoint, checks that it re-emitted minute rows to Kafka, and that ClickHouse
# collapses them to one row each. Prints PASS or FAIL per check and exits 1 if any failed.
#
# Start from empty volumes (`make reset`): data from an earlier run can make a check pass or
# fail for the wrong reason.
set -euo pipefail

cd "$(dirname "$0")/.."

POLL_S=15
LIMIT_S="${LIMIT_S:-720}"
SETTLE_S=60

# Local demo credential, not a secret.
ch() {
  curl -sf --user tb:tb-local --data-binary "$1" http://localhost:8123/
}

# True if a count query returns more than 0.
ch_positive() {
  local n
  n=$(ch "$1") && [ "${n:-0}" -gt 0 ]
}

check_aggregates() {
  ch_positive "SELECT count() FROM tb.aggregates FINAL WHERE ns='live'"
}

check_alert_log() {
  ch_positive "SELECT count() FROM tb.alert_log FINAL WHERE ns='live' AND state='firing'
    AND signal='error_rate' AND region='eu-central'"
}

check_alertmanager() {
  local body
  body=$(curl -sf 'http://localhost:9093/api/v2/alerts?filter=alertname%3D%22TenantAnomaly%22') \
    && [ -n "$body" ] && [ "$body" != "[]" ]
}

# (key, minute) pairs written to live.aggregates more than once. MinuteAgg JSON has "key"
# before "minute_ms".
reemitted_minutes() {
  docker compose exec -T -e KAFKA_HEAP_OPTS=-Xmx128m kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic live.aggregates --from-beginning --timeout-ms 10000 \
    2> /dev/null | sed -n 's/.*"key":"\([^"]*\)".*"minute_ms":\([0-9]*\).*/\1 \2/p' \
    | sort | uniq -d | wc -l | tr -d ' '
}

check_pager() {
  local n
  n=$(docker compose exec -T pager-sink wc -l /data/notifications.jsonl 2> /dev/null \
    | awk '{ print $1 }') && [ -n "$n" ] && [ "$n" -ge 1 ]
}

names=(
  ""
  "1 tb.aggregates has live rows"
  "2 tb.alert_log has a firing error_rate alert in eu-central"
  "3 Alertmanager has TenantAnomaly alerts"
  "4 pager-sink logged a notification"
  "5 tb.ingest_errors is empty"
  "6 restored job's re-emitted output collapses to one row per key and minute, and per alert and state"
)
checks=("" check_aggregates check_alert_log check_alertmanager check_pager)
passed=("" false false false false)
failures=0

pass() { echo "PASS $1"; }
fail() {
  echo "FAIL $1"
  failures=$((failures + 1))
}

start=$SECONDS
deadline=$((start + LIMIT_S))
polls=0
while :; do
  waiting=()
  for i in 1 2 3 4; do
    "${passed[i]}" && continue
    if "${checks[i]}"; then
      passed[i]=true
      pass "${names[i]} (after $((SECONDS - start)) s)"
    else
      waiting+=("$i")
    fi
  done
  [ "${#waiting[@]}" -eq 0 ] && break
  [ "$SECONDS" -lt "$deadline" ] || break
  polls=$((polls + 1))
  if [ $((polls % 4)) -eq 0 ]; then
    echo "waiting for check ${waiting[*]} ($((SECONDS - start)) s of ${LIMIT_S} s)"
  fi
  sleep "$POLL_S"
done
for i in 1 2 3 4; do
  "${passed[i]}" || fail "${names[i]} (not seen within ${LIMIT_S} s)"
done

errors=$(ch "SELECT count() FROM tb.ingest_errors") || errors=unknown
if [ "$errors" = 0 ]; then
  pass "${names[5]}"
else
  fail "${names[5]} ($errors rows)"
fi

echo "restoring the job from a checkpoint"
if docker compose exec -T jobmanager bash /opt/tb-scripts/submit-job.sh --from-checkpoint; then
  echo "waiting ${SETTLE_S} s for the restored job's output"
  sleep "$SETTLE_S"
  reemitted=$(reemitted_minutes) || reemitted=unknown
  agg_extra=$(ch "SELECT count() - uniqExact(key, minute) FROM tb.aggregates FINAL WHERE ns='live'") \
    || agg_extra=unknown
  alert_extra=$(ch "SELECT count() - uniqExact(alert_id, state) FROM tb.alert_log FINAL WHERE ns='live'") \
    || alert_extra=unknown
  detail="re-emitted minute rows $reemitted; extra rows: aggregates $agg_extra, alert_log $alert_extra"
  if [ "$reemitted" = unknown ]; then
    fail "${names[6]} (could not read live.aggregates; $detail)"
  elif [ "$reemitted" -eq 0 ]; then
    fail "${names[6]} (nothing was re-emitted, so nothing was tested; $detail)"
  elif [ "$agg_extra" = 0 ] && [ "$alert_extra" = 0 ]; then
    pass "${names[6]} ($detail)"
  else
    fail "${names[6]} ($detail)"
  fi
else
  fail "${names[6]} (resubmit failed)"
fi

if [ "$failures" -gt 0 ]; then
  echo "smoke: $failures of 6 checks failed"
  exit 1
fi
echo "smoke: all 6 checks passed"
