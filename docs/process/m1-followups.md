# Milestone 1 follow-ups

Planning material, like the rest of `docs/process/`. These are review findings from milestone 1 that were deferred on purpose, each with the milestone where it should land. None blocked the merge.

## Milestone 5

- done (milestone 5): Measure pipeline delay from `min(emitted_at)` on the non-`FINAL` alert log. Re-emitted alerts carry a later `emitted_at`, and `FINAL` keeps the later row.
- done (milestone 5): Delete retained checkpoints of cancelled jobs. (bench jobs only; the live job's retained checkpoints are moved to milestone 6, see m3-followups.)
- done (milestone 5): Revisit the TaskManager memory (2560m) for benchmark state sizes. (measured: 76.0% and 79.6% of the limit, no restart or OOM kill; kept at 2560m.)
- done (milestone 5): Scorer writes machine state every minute even when nothing changed; measure before optimising. (measured as busy time per vertex; no optimisation.)
- done (milestone 5): Generator: delivery failures surface only after `delivery.timeout.ms` (300 s); consider `error_cb` or a lower timeout. (bench script only; the demo generator's timeout is moved to milestone 6.)

## Milestone 6 (README and compose pass)

Decided in the milestone 6 plan's follow-up table (`docs/process/plans/2026-11-13-m6-readme-design.md`). What stays open is in `open-items.md`.

- Alertmanager state lives on an anonymous volume, so silences are lost when the container is recreated.
- `tb.ingest_errors` has no TTL.
- The Python image runs as root.
- The JobManager and TaskManager don't wait for Kafka. `make demo` submits after `wait-ready.sh`, so this is fine today.
- Host Kafka clients try IPv6 first; set `broker.address.family=v4`.
- `uv sync` in CI and `make ci` should use `--locked`.
- The shaded jar keeps merged NOTICE files but drops bundled dependencies' own LICENSE files (zstd-jni is BSD). Add a third-party notice file before the jar is ever published.
- The forwarder posts all due alerts in one request, so one rejected entry blocks the batch. Post per alert, or log the response body.
