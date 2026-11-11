#!/usr/bin/env bash
# Submit one bounded eval job, detached. Runs inside the jobmanager container:
#
#   docker compose exec -T jobmanager bash /opt/tb-scripts/submit-eval.sh \
#     <run_id> <mode> <input_topic> <replay_end_ms> <thresholds_path> [extra flink option...]
#
# <mode> is aggregates or replay. Each run checkpoints under its own directory,
# /flink-data/checkpoints/eval/<run_id>, and a run id is never reused. The extra options go to
# `flink run` before the jar (for example -Dstate.backend.type=hashmap). flink's own output goes
# to stderr; the job id is printed alone on the last line of stdout.
set -euo pipefail

JAR=/opt/tb-job/target/tenant-baselines-job.jar
CHECKPOINTS=/flink-data/checkpoints/eval

fail() {
  echo "submit-eval: $*" >&2
  exit 1
}

if [ "$#" -lt 5 ]; then
  echo "usage: $0 <run_id> <mode> <input_topic> <replay_end_ms> <thresholds_path> [extra...]" >&2
  exit 2
fi
run_id=$1
mode=$2
input_topic=$3
replay_end_ms=$4
thresholds=$5
shift 5

[[ "$run_id" =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]] || fail "run id may hold only letters, digits, . and -: $run_id"
case "$mode" in
  aggregates | replay) ;;
  *) fail "mode must be aggregates or replay, got $mode" ;;
esac
[[ "$replay_end_ms" =~ ^[0-9]+$ ]] || fail "replay end must be epoch milliseconds, got $replay_end_ms"
[ -f "$JAR" ] || fail "no job jar at $JAR; run make jar"
[ -f "$thresholds" ] || fail "no thresholds file at $thresholds"
[ ! -e "$CHECKPOINTS/$run_id" ] || fail "$CHECKPOINTS/$run_id exists; a run id is never reused"

out=$(flink run -d -Dexecution.checkpointing.dir="file://$CHECKPOINTS/$run_id" "$@" "$JAR" \
  --bootstrap kafka:19092 --ns eval --run-id "$run_id" --mode "$mode" \
  --input-topic "$input_topic" --replay-end-ms "$replay_end_ms" --bounded \
  --thresholds "$thresholds" --seasonal-k 3,4,5,6 --live-setting k4 --group-id "tb-$run_id") \
  || { echo "$out" >&2; fail "flink run failed"; }
echo "$out" >&2
job_id=$(sed -n 's/.*JobID \([0-9a-f]\{32\}\).*/\1/p' <<< "$out" | tail -n 1)
[ -n "$job_id" ] || fail "no job id in the output of flink run"
echo "$job_id"
