#!/usr/bin/env bash
# End-to-end smoke test of the grouped demo; `make smoke` runs it after `make demo`, which refuses
# to start unless the stack is empty (`make reset`). Checks 1-8 are polled every 15 s for up to
# 30 minutes: the demo script's tenant drop, error burst and regional error spike start 2, 3 and
# 11 minutes after the generator catches up. Check 5 can be observed only during the scripted
# spike, from about 17 to 26 minutes after the generator catches up. Checks 9-12 then run once,
# in order. Check 11 restores the job from its oldest retained checkpoint and checks that this
# restore re-emitted output and that it collapses to one row each. Check 12 stops the generator,
# restores the job during the total outage and waits for it to page; it leaves the generator
# stopped.
#
# CHECKS, a space-separated list of check numbers, runs only those, and only they count; unset or
# empty runs all 12. To rerun checks 11 and 12 on a demo stack that has run check 12:
#   1. docker compose --profile demo start generator. Both checks need it running. It replays
#      its three weeks of history (about 24.3 million events), which the running job drops as
#      late, and then starts a new demo script.
#   2. Wait until scripts/late-events.sh stops rising, then at least 90 s more, so that the
#      oldest retained checkpoint comes after the replay.
#   3. CHECKS="11 12" scripts/smoke.sh
# After that replay, check 10 cannot pass on that stack. The new demo script has a new spike,
# but a tenant counts as active only with 1200 requests over the last 60 minutes, and the outage
# removed them, so checks 2-8 are meant for a fresh stack (make reset).
#
# Prints PASS or FAIL per check and exits 1 if any failed.
set -euo pipefail

cd "$(dirname "$0")/.."

ALL_CHECKS="1 2 3 4 5 6 7 8 9 10 11 12"
CHECKS="${CHECKS:-$ALL_CHECKS}"
POLL_S=15
LIMIT_S="${LIMIT_S:-1800}"
# After a restore no minute closes until every partition has delivered an event or been silent
# for 60 s, so the restored job's output needs about two minutes.
RESTORE_SETTLE_S=120
# Every partition goes idle after 60 s without an event.
OUTAGE_SILENCE_S=90
OUTAGE_LIMIT_S=900

selected=" "
for c in $CHECKS; do
  case " $ALL_CHECKS " in
    *" $c "*) selected+="$c " ;;
    *)
      echo "smoke: CHECKS has '$c'; use check numbers from 1 to 12" >&2
      exit 2
      ;;
  esac
done
if [ "$selected" = " " ]; then
  echo "smoke: CHECKS has no check numbers; leave it empty to run all 12" >&2
  exit 2
fi

wanted() {
  case "$selected" in
    *" $1 "*) return 0 ;;
    *) return 1 ;;
  esac
}

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
LIVE="dateDiff('second', event_time, emitted_at) < 600"

# Live minute rows, and the history reaches back three weeks.
check_aggregates() {
  ch_positive "SELECT count() > 0 AND min(minute) <= now() - INTERVAL 20 DAY
    FROM tb.aggregates WHERE ns='live'"
}

check_drop_alert() {
  ch_positive "SELECT count() FROM tb.alert_log FINAL WHERE ns='live' AND scope='tenant'
    AND state='firing' AND detector='seasonal' AND signal='volume_5m' AND unit LIKE '%-d-%'
    AND $LIVE"
}

check_regional_alert() {
  ch_positive "SELECT count() FROM tb.alert_log FINAL WHERE ns='live' AND scope='region'
    AND state='firing' AND detector='seasonal' AND signal='error_rate' AND $LIVE"
}

check_absorbed() {
  ch_positive "SELECT count() FROM tb.alert_log FINAL WHERE ns='live' AND state='absorbed'
    AND detector='seasonal' AND signal='error_rate' AND reason LIKE 'regional:%' AND $LIVE"
}

# An error_rate tenant alert that Alertmanager reports as inhibited.
check_inhibit() {
  curl -sf http://localhost:9093/api/v2/alerts | python3 -c '
import json
import sys

alerts = json.load(sys.stdin)
sys.exit(0 if any(
    a["labels"].get("scope") == "tenant"
    and a["labels"].get("signal") == "error_rate"
    and a["status"].get("inhibitedBy")
    for a in alerts
) else 1)'
}

# True if pager-sink logged a notification to the given webhook path.
paged() {
  docker compose exec -T pager-sink grep -qF "\"receiver\": \"$1\"" /data/notifications.jsonl \
    2> /dev/null
}

check_pagers() {
  paged /regional && paged /tenant
}

check_scores() {
  ch_positive "SELECT uniqExact(signal) = 3 FROM tb.scores FINAL
    WHERE ns='live' AND median IS NOT NULL"
}

# A dedicated unit with a live drain and a later undrain by a regional alert.
check_unit_status() {
  ch_positive "SELECT count() FROM (
      SELECT unit FROM tb.unit_status FINAL
      WHERE ns='live' AND unit LIKE '%-d-%' AND dateDiff('second', at, emitted_at) < 600
      GROUP BY unit
      HAVING countIf(status = 'drain') > 0
        AND minIf(at, status = 'drain') < maxIf(at, status = 'undrain' AND reason = 'regional'))"
}

# (key, minute) pairs written to live.aggregates more than once, over the whole topic. MinuteAgg
# JSON has "key" before "minute_ms".
reemitted_minutes() {
  docker compose exec -T -e KAFKA_HEAP_OPTS=-Xmx128m kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic live.aggregates --from-beginning --timeout-ms 10000 \
    2> /dev/null | sed -n 's/.*"key":"\([^"]*\)".*"minute_ms":\([0-9]*\).*/\1 \2/p' \
    | sort | uniq -d | wc -l | tr -d ' '
}

# A zero-count minute that starts at least 5 minutes after the given epoch second.
outage_zero_minute() {
  ch_positive "SELECT count() FROM tb.aggregates WHERE ns='live' AND count = 0
    AND minute >= toDateTime($1, 'UTC') + INTERVAL 5 MINUTE"
}

# A live seasonal volume_5m alert, tenant or regional, whose event time is after the given
# epoch second.
outage_alert() {
  ch_positive "SELECT count() FROM tb.alert_log FINAL WHERE ns='live' AND state='firing'
    AND detector='seasonal' AND signal='volume_5m' AND event_time > toDateTime($1, 'UTC')
    AND $LIVE"
}

names=(
  ""
  "1 tb.aggregates has live rows reaching back 20 days"
  "2 tb.alert_log has a seasonal volume_5m tenant alert on a dedicated unit, decided live"
  "3 tb.alert_log has a seasonal error_rate regional alert, decided live"
  "4 tb.alert_log has seasonal error_rate tenant alerts absorbed by a regional alert, decided live"
  "5 Alertmanager inhibits an error_rate tenant alert"
  "6 pager-sink logged notifications to the /regional and the /tenant receiver"
  "7 tb.scores has a median for each of the three signals"
  "8 tb.unit_status has a live drain of a dedicated unit and a later regional undrain"
  "9 tb.ingest_errors is empty"
  "10 the job dropped no events as late"
  "11 a job restored from its oldest checkpoint re-emits output that collapses to one row each"
  "12 a total outage across a restore closes zero-count minutes and pages a volume_5m alert"
)
checks=("" check_aggregates check_drop_alert check_regional_alert check_absorbed check_inhibit
  check_pagers check_scores check_unit_status)
passed=("" false false false false false false false false)
polled=()
for i in 1 2 3 4 5 6 7 8; do
  if wanted "$i"; then
    polled+=("$i")
  fi
done
ran=0
failures=0

pass() {
  echo "PASS $1"
  ran=$((ran + 1))
}
fail() {
  echo "FAIL $1"
  ran=$((ran + 1))
  failures=$((failures + 1))
}

poll_checks() {
  local start=$SECONDS
  local deadline=$((start + LIMIT_S))
  local polls=0 i waiting
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
}

check_ingest_errors() {
  local errors
  errors=$(ch "SELECT count() FROM tb.ingest_errors") || errors=unknown
  if [ "$errors" = 0 ]; then
    pass "${names[9]}"
  else
    fail "${names[9]} ($errors rows)"
  fi
}

check_late() {
  local late
  late=$(scripts/late-events.sh) || late=unknown
  if [ "$late" = 0 ]; then
    pass "${names[10]} (lateEvents $late)"
  else
    fail "${names[10]} (lateEvents $late)"
  fi
}

check_restore() {
  local before after reemitted agg_extra scores_extra alert_extra late detail
  # Earlier restores left re-emitted minutes in the topic too, so count this restore's own.
  if ! before=$(reemitted_minutes); then
    fail "${names[11]} (could not read live.aggregates before the restore)"
    return
  fi
  echo "restoring the job from its oldest retained checkpoint"
  if ! docker compose exec -T jobmanager bash /opt/tb-scripts/submit-job.sh --from-checkpoint; then
    fail "${names[11]} (resubmit failed)"
    return
  fi
  echo "waiting ${RESTORE_SETTLE_S} s for the restored job's output"
  sleep "$RESTORE_SETTLE_S"
  if after=$(reemitted_minutes); then
    reemitted=$((after - before))
  else
    reemitted=unknown
  fi
  agg_extra=$(ch "SELECT count() - uniqExact(key, minute) FROM tb.aggregates FINAL WHERE ns='live'") \
    || agg_extra=unknown
  scores_extra=$(ch "SELECT count() - uniqExact(key, signal, minute) FROM tb.scores FINAL
    WHERE ns='live'") || scores_extra=unknown
  # Business columns, not the dedup key alert_id, so an alert re-emitted with another id counts.
  alert_extra=$(ch "SELECT count()
      - uniqExact(detector, setting, scope, key, signal, starts_at, state)
    FROM tb.alert_log FINAL WHERE ns='live'") || alert_extra=unknown
  # The restored job starts on a backlog at least 60 s deep while traffic is live.
  late=$(scripts/late-events.sh) || late=unknown
  detail="minute rows re-emitted by this restore $reemitted (topic total ${after:-unknown});"
  detail+=" extra rows: aggregates $agg_extra, scores $scores_extra, alert_log $alert_extra;"
  detail+=" restored job's lateEvents $late"
  if [ "$reemitted" = unknown ]; then
    fail "${names[11]} (could not read live.aggregates; $detail)"
  elif [ "$reemitted" -le 0 ]; then
    fail "${names[11]} (nothing was re-emitted, so nothing was tested; $detail)"
  elif [ "$agg_extra" = 0 ] && [ "$scores_extra" = 0 ] && [ "$alert_extra" = 0 ] \
    && [ "$late" = 0 ]; then
    pass "${names[11]} ($detail)"
  else
    fail "${names[11]} ($detail)"
  fi
}

# "after <n> s", or "not seen" when n is empty.
seen_after() {
  if [ -n "$1" ]; then
    echo "after $1 s"
  else
    echo "not seen"
  fi
}

check_outage() {
  local stop deadline zero_s="" alert_s="" detail
  echo "stopping the generator: a total outage"
  if ! docker compose --profile demo stop generator; then
    fail "${names[12]} (could not stop the generator)"
    return
  fi
  stop=$(date +%s)
  echo "waiting ${OUTAGE_SILENCE_S} s so every partition is silent"
  sleep "$OUTAGE_SILENCE_S"
  echo "restoring the job from its newest checkpoint during the outage"
  if ! docker compose exec -T jobmanager bash /opt/tb-scripts/submit-job.sh; then
    fail "${names[12]} (resubmit failed)"
    return
  fi
  deadline=$((SECONDS + OUTAGE_LIMIT_S))
  while :; do
    if [ -z "$zero_s" ] && outage_zero_minute "$stop"; then
      zero_s=$(($(date +%s) - stop))
    fi
    if [ -z "$alert_s" ] && outage_alert "$stop"; then
      alert_s=$(($(date +%s) - stop))
    fi
    [ -n "$zero_s" ] && [ -n "$alert_s" ] && break
    [ "$SECONDS" -lt "$deadline" ] || break
    sleep "$POLL_S"
  done
  detail="from the generator stop: zero-count minute $(seen_after "$zero_s"),"
  detail+=" volume_5m alert $(seen_after "$alert_s")"
  if [ -n "$zero_s" ] && [ -n "$alert_s" ]; then
    pass "${names[12]} ($detail)"
  else
    fail "${names[12]} ($detail; polled ${OUTAGE_LIMIT_S} s after the restore)"
  fi
}

if [ "${#polled[@]}" -gt 0 ]; then
  poll_checks
fi
if wanted 9; then check_ingest_errors; fi
if wanted 10; then check_late; fi
if wanted 11; then check_restore; fi
if wanted 12; then check_outage; fi

if [ "$failures" -gt 0 ]; then
  echo "smoke: $failures of $ran checks failed"
  exit 1
fi
echo "smoke: all $ran checks passed"
