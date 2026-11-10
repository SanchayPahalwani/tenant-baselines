# Milestone 1 follow-ups

Planning material, like the rest of `docs/superpowers/`. These are review findings from milestone 1 that were deferred on purpose, each with the milestone where it should land. None blocked the merge.

## Spec wording to update (owner's approval, at the start of milestone 2)

- **Forwarder stale rule (§4.10).** As built, the forwarder skips an alert *decided* more than 10 minutes after its event time (`emitted_at − event_time > 10 min`). The spec says alerts that *started* more than 10 minutes ago. The built rule keeps paging a real incident that is still firing when the forwarder restarts.
- **Incident size convention.** `volume_drop` magnitude is the fraction of traffic removed, while §5.4 writes "volume ×0.3". Rename the field to `volume_mult` before milestone 4 writes scenario code.

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
  - count = 20 boundary;
  - calm-streak pause;
  - `Level.judge` boundaries;
  - per-key overrides and validity windows;
  - the forwarder's immediate resolve send;
  - non-JSON webhook bodies;
  - the 600 s stale boundary.
- **Generator:**
  - `producer.flush()` and Ctrl-C handling;
  - a clamp on `volume_drop` above 1;
  - the per-key rate formula is written twice;
  - the parameter name `load` shadows a function.
- **Scripts:**
  - `make smoke` doesn't enforce `make reset`;
  - the default submit misses jobs in CREATED or INITIALIZING;
  - `make ci` and `ci.yml` are kept in sync by hand;
  - the Flink CLI prints `Unknown module: jdk.compiler` from the image;
  - the demo hint "about 3 minutes" should be re-measured in milestone 2.
