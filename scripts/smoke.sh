#!/usr/bin/env bash
# End-to-end smoke test of the demo; `make smoke` runs it after `make demo`, which refuses to
# start unless the stack is empty (`make reset`). Checks 1-6 are polled every 15 s for up to
# 30 minutes: the demo script's tenant drop and regional error spike start 2 and 14 minutes
# after the generator catches up. Checks 7 and 8 run once after them. Check 9 restores the job
# from a checkpoint, checks that it re-emitted minute rows to Kafka, and that ClickHouse collapses
# them to one row each. Prints PASS or FAIL per check and exits 1 if any failed.
set -euo pipefail

cd "$(dirname "$0")/.."

POLL_S=15
LIMIT_S="${LIMIT_S:-1800}"
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

# An alert decided within 10 minutes of its event time was decided live, not in the history
# (the forwarder's stale rule).
DECIDED_LIVE="dateDiff('second', event_time, emitted_at) < 600"

# Live minute rows, and the history reaches back three weeks.
check_aggregates() {
  ch_positive "SELECT count() > 0 AND min(minute) <= now() - INTERVAL 20 DAY
    FROM tb.aggregates WHERE ns='live'"
}

check_drop_alert() {
  ch_positive "SELECT count() FROM tb.alert_log FINAL WHERE ns='live' AND state='firing'
    AND detector='seasonal' AND signal='volume_5m' AND unit LIKE '%-d-%' AND $DECIDED_LIVE"
}

check_spike_alert() {
  ch_positive "SELECT count() FROM tb.alert_log FINAL WHERE ns='live' AND state='firing'
    AND detector='seasonal' AND signal='error_rate' AND $DECIDED_LIVE"
}

check_alertmanager() {
  local body
  body=$(curl -sf 'http://localhost:9093/api/v2/alerts?filter=detector%3D%22seasonal%22') \
    && [ -n "$body" ] && [ "$body" != "[]" ]
}

check_scores() {
  ch_positive "SELECT uniqExact(signal) = 3 FROM tb.scores FINAL
    WHERE ns='live' AND median IS NOT NULL"
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
  "1 tb.aggregates has live rows reaching back 20 days"
  "2 tb.alert_log has a seasonal volume_5m alert on a dedicated unit, decided live"
  "3 tb.alert_log has a seasonal error_rate alert, decided live"
  "4 Alertmanager has seasonal alerts"
  "5 pager-sink logged a notification"
  "6 tb.scores has a median for each of the three signals"
  "7 tb.ingest_errors is empty"
  "8 the job dropped no events as late"
  "9 restored job's re-emitted output collapses to one row each in aggregates, scores and alert_log"
)
polled=(1 2 3 4 5 6)
checks=("" check_aggregates check_drop_alert check_spike_alert check_alertmanager check_pager check_scores)
passed=("" false false false false false false)
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
  for i in "${polled[@]}"; do
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
for i in "${polled[@]}"; do
  "${passed[i]}" || fail "${names[i]} (not seen within ${LIMIT_S} s)"
done

errors=$(ch "SELECT count() FROM tb.ingest_errors") || errors=unknown
if [ "$errors" = 0 ]; then
  pass "${names[7]}"
else
  fail "${names[7]} ($errors rows)"
fi

late=$(scripts/late-events.sh) || late=unknown
if [ "$late" = 0 ]; then
  pass "${names[8]} (lateEvents $late)"
else
  fail "${names[8]} (lateEvents $late)"
fi

echo "restoring the job from a checkpoint"
if docker compose exec -T jobmanager bash /opt/tb-scripts/submit-job.sh --from-checkpoint; then
  echo "waiting ${SETTLE_S} s for the restored job's output"
  sleep "$SETTLE_S"
  reemitted=$(reemitted_minutes) || reemitted=unknown
  agg_extra=$(ch "SELECT count() - uniqExact(key, minute) FROM tb.aggregates FINAL WHERE ns='live'") \
    || agg_extra=unknown
  scores_extra=$(ch "SELECT count() - uniqExact(key, signal, minute) FROM tb.scores FINAL WHERE ns='live'") \
    || scores_extra=unknown
  alert_extra=$(ch "SELECT count() - uniqExact(alert_id, state) FROM tb.alert_log FINAL WHERE ns='live'") \
    || alert_extra=unknown
  detail="re-emitted minute rows $reemitted; extra rows: aggregates $agg_extra, scores $scores_extra, alert_log $alert_extra"
  if [ "$reemitted" = unknown ]; then
    fail "${names[9]} (could not read live.aggregates; $detail)"
  elif [ "$reemitted" -eq 0 ]; then
    fail "${names[9]} (nothing was re-emitted, so nothing was tested; $detail)"
  elif [ "$agg_extra" = 0 ] && [ "$scores_extra" = 0 ] && [ "$alert_extra" = 0 ]; then
    pass "${names[9]} ($detail)"
  else
    fail "${names[9]} ($detail)"
  fi
else
  fail "${names[9]} (resubmit failed)"
fi

if [ "$failures" -gt 0 ]; then
  echo "smoke: $failures of 9 checks failed"
  exit 1
fi
echo "smoke: all 9 checks passed"
