# Milestone 1 follow-ups

Planning material, like the rest of `docs/superpowers/`. These are review findings from milestone 1 that were deferred on purpose, each with the milestone where it should land. None blocked the merge.

## Before milestone 4 runs a second job on the cluster

- `scripts/submit-job.sh` restores from the newest checkpoint of *any* job. Scope checkpoint directories per namespace (for example `-Dexecution.checkpointing.dir=file:///flink-data/checkpoints/<ns>`).
- `ThresholdsFile`: reject unknown signal names and missing fields with a message that names the set, check fire vs resolve order per direction, and reject duplicate active detector/setting pairs. Close a firing alert when its set's `valid_to` passes.
- `EventJson`: reject a tenant, region or unit containing `|`, so keys stay unambiguous for per-key threshold maps.
- `LatencyHistogram`: choose a policy for `p95` when counts sum below `n`, and for NaN or negative latencies. The Python aggregator in the milestone 4 equivalence check must match it.

## Milestone 5

- Measure pipeline delay from `min(emitted_at)` on the non-`FINAL` alert log. Re-emitted alerts carry a later `emitted_at`, and `FINAL` keeps the later row.
- Watch `lateEvents` across restarts. After a restore, the live watermark can latch before an unread backlog is consumed.
- Delete retained checkpoints of cancelled jobs.
- Revisit the TaskManager memory (2560m) for benchmark state sizes.
- Scorer writes machine state every minute even when nothing changed; measure before optimising.
- Generator: delivery failures surface only after `delivery.timeout.ms` (300 s); consider `error_cb` or a lower timeout.

## Milestone 6 (README and compose pass)

- Alertmanager state lives on an anonymous volume, so silences are lost when the container is recreated.
- `tb.ingest_errors` has no TTL.
- The Python image runs as root.
- The JobManager and TaskManager don't wait for Kafka. `make demo` submits after `wait-ready.sh`, so this is fine today.
- Host Kafka clients try IPv6 first; set `broker.address.family=v4`.
- `uv sync` in CI and `make ci` should use `--locked`.
- The shaded jar keeps merged NOTICE files but drops bundled dependencies' own LICENSE files (zstd-jni is BSD). Add a third-party notice file before the jar is ever published.
- The forwarder posts all due alerts in one request, so one rejected entry blocks the batch. Post per alert, or log the response body.

## When the code is next touched

- **Tests:**
  - late events not re-aggregated after a later watermark;
  - `@Timeout` in separate-thread mode;
  - dimension fields on emitted minutes;
  - an on-time out-of-order event;
  - watermark latch boundaries;
  - `Watermarks` factories;
  - per-key overrides and validity windows;
  - non-JSON webhook bodies.
- **Scripts:**
  - the default submit misses jobs in CREATED or INITIALIZING;
  - `make ci` and `ci.yml` are kept in sync by hand;
  - the Flink CLI prints `Unknown module: jdk.compiler` from the image.
