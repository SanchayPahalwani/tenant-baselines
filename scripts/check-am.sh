#!/usr/bin/env bash
# Checks infra/alertmanager/alertmanager.yml with amtool from the image the stack uses:
# the config parses and has one inhibit rule, and the two pager routes pick the right receivers.
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE=prom/alertmanager:v0.34.2
amtool() {
  docker run --rm -v "$PWD/infra/alertmanager":/cfg:ro --entrypoint amtool "$IMAGE" "$@"
}

fail() {
  echo "check-am FAILED: $1" >&2
  exit 1
}

out=$(amtool check-config /cfg/alertmanager.yml 2>&1) || { echo "$out" >&2; fail "check-config rejects alertmanager.yml"; }
echo "$out"
grep -qw '1 inhibit rules' <<<"$out" || fail "check-config does not report '1 inhibit rules'"

amtool config routes test --config.file=/cfg/alertmanager.yml --verify.receivers=regional-pager \
  alertname=RegionAnomaly scope=region region=ap-southeast signal=error_rate \
  || fail "a regional alert does not route to regional-pager"

amtool config routes test --config.file=/cfg/alertmanager.yml --verify.receivers=tenant-pager \
  alertname=TenantAnomaly scope=tenant region=ap-southeast tenant=t09 signal=error_rate \
  || fail "a tenant alert does not route to tenant-pager"

echo "check-am OK"
