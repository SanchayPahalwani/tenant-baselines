#!/usr/bin/env bash
# Submit one unbounded benchmark job, detached. Runs inside the jobmanager container:
#
#   docker compose exec -T jobmanager bash /opt/tb-scripts/submit-bench.sh \
#     <run_id> <input_topic> [extra flink option...]
#
# The job runs the live pipeline on the build in target/, with the demo thresholds and score
# output on. Each run checkpoints under its own directory, /flink-data/checkpoints/bench/<run_id>,
# and a run id is never reused. The extra options go to `flink run` before the jar. flink's own
# output goes to stderr; the job id is printed alone on the last line of stdout.
set -euo pipefail

JAR=/opt/tb-job/target/tenant-baselines-job.jar
CHECKPOINTS=/flink-data/checkpoints/bench
THRESHOLDS=/opt/tb-config/thresholds-demo.json

fail() {
  echo "submit-bench: $*" >&2
  exit 1
}

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <run_id> <input_topic> [extra...]" >&2
  exit 2
fi
run_id=$1
input_topic=$2
shift 2

[[ "$run_id" =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]] || fail "run id may hold only letters, digits, . and -: $run_id"
[ -f "$JAR" ] || fail "no job jar at $JAR"
[ ! -e "$CHECKPOINTS/$run_id" ] || fail "$CHECKPOINTS/$run_id exists; a run id is never reused"

out=$(flink run -d -Dexecution.checkpointing.dir="file://$CHECKPOINTS/$run_id" "$@" "$JAR" \
  --bootstrap kafka:19092 --ns bench --run-id "$run_id" --mode live \
  --input-topic "$input_topic" --thresholds "$THRESHOLDS" \
  --seasonal-k 4 --live-setting k4 --scores true --group-id "tb-$run_id") \
  || { echo "$out" >&2; fail "flink run failed"; }
echo "$out" >&2
job_id=$(sed -n 's/.*JobID \([0-9a-f]\{32\}\).*/\1/p' <<< "$out" | tail -n 1)
[ -n "$job_id" ] || fail "no job id in the output of flink run"
echo "$job_id"
