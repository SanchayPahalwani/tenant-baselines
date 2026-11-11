#!/usr/bin/env bash
# Submit the live job, replacing any running copy. Runs inside the jobmanager container:
#
#   docker compose exec -T jobmanager bash /opt/tb-scripts/submit-job.sh [--from-checkpoint]
#
# By default it cancels any running or restarting tenant-baselines-live job (checkpoints are
# retained on cancel) and submits a new one restored from the newest retained checkpoint of any
# live job, so alerts that are firing carry over and get their resolved events. Only when there is
# no checkpoint does it start with empty state, from the consumer group's committed offsets. It
# prints which of the two it did.
#
# --from-checkpoint waits until the running job has completed 3 checkpoints, cancels it
# (checkpoints are retained on cancel) and restores the new job from the oldest retained one.
# Flink keeps 3 checkpoints taken 30 s apart (docker-compose.yml), so that one is at least 60 s
# old: the new job replays at least one minute close from its Kafka offsets and re-emits output
# that was already written. The latest checkpoint is at most 30 s old, so a restore from it
# re-emits nothing about half the time.
set -euo pipefail

JOB_NAME=tenant-baselines-live
JAR=/opt/tb-job/target/tenant-baselines-job.jar
CHECKPOINT_DIR=/flink-data/checkpoints/live
REST=http://localhost:8081
RETAINED=3
TIMEOUT="${TIMEOUT:-180}"

from_checkpoint=false
case "$#:${1:-}" in
  0:) ;;
  1:--from-checkpoint) from_checkpoint=true ;;
  *)
    echo "usage: $0 [--from-checkpoint]" >&2
    exit 2
    ;;
esac

fail() {
  echo "submit-job: $*" >&2
  exit 1
}

# IDs of the jobs named $JOB_NAME in the given states, from `flink list -r` output
# (running and restarting jobs, one per line as "<start time> : <job id> : <name> (<state>)").
ids_in_state() {
  local listing=$1
  shift
  awk -F' : ' -v name="$JOB_NAME" -v states="$*" '
    BEGIN { n = split(states, s, " "); for (i = 1; i <= n; i++) want[name " (" s[i] ")"] = 1 }
    NF == 3 && ($3 in want) { print $2 }
  ' <<< "$listing"
}

job_state() {
  wget -qO- "$REST/jobs/$1" | grep -o '"state":"[A-Z_]*"' | sed -n '1s/.*:"\(.*\)"/\1/p'
}

is_cancelled() {
  [ "$(job_state "$1")" = CANCELED ]
}

has_retained_checkpoints() {
  local n
  n=$(wget -qO- "$REST/jobs/$1/checkpoints" | grep -o '"counts":{[^}]*}' \
    | sed -n 's/.*"completed":\([0-9]*\).*/\1/p') && [ "${n:-0}" -ge "$RETAINED" ]
}

# Runs a check every 2 s until it succeeds, for up to $TIMEOUT seconds.
wait_for() {
  local what=$1
  shift
  local deadline=$((SECONDS + TIMEOUT))
  until "$@"; do
    [ "$SECONDS" -lt "$deadline" ] || fail "timed out after ${TIMEOUT}s waiting for $what"
    sleep 2
  done
}

cancel() {
  flink cancel "$1"
  wait_for "job $1 to be cancelled" is_cancelled "$1"
}

# The job's chk-N directories that have their _metadata file, oldest first.
retained_checkpoints() {
  local dir
  for dir in "$CHECKPOINT_DIR/$1"/chk-*; do
    if [ -f "$dir/_metadata" ]; then
      echo "${dir##*/chk-} $dir"
    fi
  done | sort -n | cut -d' ' -f2
}

# The newest chk-N directory with its _metadata file, of any live job; nothing if there is none.
newest_checkpoint() {
  [ -d "$CHECKPOINT_DIR" ] || return 0
  find "$CHECKPOINT_DIR" -mindepth 3 -maxdepth 3 -path '*/chk-*/_metadata' -printf '%T@ %h\n' \
    | sort -n | tail -n 1 | cut -d' ' -f2-
}

submit() {
  flink run -d -Dexecution.checkpointing.dir=file:///flink-data/checkpoints/live "$@" "$JAR" \
    --bootstrap kafka:19092 --ns live --run-id live --mode live \
    --thresholds /opt/tb-config/thresholds-demo.json
}

[ -f "$JAR" ] || fail "no job jar at $JAR; run make jar"
listing=$(flink list -r) || fail "could not list jobs"

if $from_checkpoint; then
  mapfile -t ids < <(ids_in_state "$listing" RUNNING)
  [ "${#ids[@]}" -eq 1 ] || fail "expected one RUNNING $JOB_NAME job to restore from, found ${#ids[@]}"
  id=${ids[0]}
  echo "waiting for job $id to complete $RETAINED checkpoints"
  wait_for "$RETAINED completed checkpoints of job $id" has_retained_checkpoints "$id"
  cancel "$id"
  mapfile -t retained < <(retained_checkpoints "$id")
  [ "${#retained[@]}" -ge "$RETAINED" ] \
    || fail "expected $RETAINED checkpoints (chk-N with _metadata) under $CHECKPOINT_DIR/$id, found ${#retained[@]}"
  checkpoint=${retained[0]}
  echo "restoring from $checkpoint"
  submit -s "$checkpoint"
else
  mapfile -t ids < <(ids_in_state "$listing" RUNNING RESTARTING)
  for id in "${ids[@]}"; do
    cancel "$id"
  done
  checkpoint=$(newest_checkpoint)
  if [ -n "$checkpoint" ]; then
    echo "restoring from the newest checkpoint, $checkpoint"
    submit -s "$checkpoint" || fail "could not restore from $checkpoint"
  else
    echo "no checkpoint under $CHECKPOINT_DIR; starting with empty state"
    submit
  fi
fi
