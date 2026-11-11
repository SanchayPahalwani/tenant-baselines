# Milestone 2 follow-ups

Planning material, like the rest of `docs/superpowers/`. These are review findings from milestone 2 that were deferred on purpose, grouped by the milestone where each should land. None blocked the merge.

## Milestone 4

- Guard against a NaN magnitude in the incident code.
- `test_key_rate_splits_tenant_rate` uses an exact float compare. Nothing asserts that `tick` uses `key_rate_rpm`.
- Missing tests: `Scores.fireReference`, a negative DOWN score, NaN inputs, the median prefix contract, a negative `hourStart`.
- A late put for an older week wipes the newer week in `BucketHistory`. Ignore it when `week < slotWeek`.
- `BucketHistory` and `HourCache` tests: the exactly-30 boundary, a cross-signal wipe, the asserted week id.
- `VolumeRing`: a test with differing per-minute scaled MADs across an hour boundary.
- `PopulationBaselines` tests: the count == 20 boundary, a null p95, and `@Timeout` in `SEPARATE_THREAD` mode.
- `currentWeekDoesNotMoveTheBaseline` cannot fail for its stated property. Rename or rework it, and add a Scorer-level test that minutes while an alert fires still feed the baseline.
- A firing seasonal volume alert resolves on entering a cold bucket, because `ZeroTraffic` turns UNSCORED into CALM when a trailing expectation exists. It re-fires in the next warm bucket. This only affects keys in their second or third week. Keep UNSCORED while firing for a non-zero-traffic reason, or document the behaviour.

## Milestone 5

- The union adds one local channel hop per event. Measure it in the benchmark.
- The fixed-rate benchmark keeps every partition busy, so its delay figures see a watermark lag of about 21 s. The write-up must say that a sparse partition adds up to 60 s.
- A tenant alert absorbed by two regional alerts keeps only the later `absorbed` row in `tb.alert_log`.
- Stale week-4 histogram entries leak in `PopulationBaselines` when a bucket's hour gets no scored minute for a week. Sweep weeks below `w - 4` on each timer.
- No test drives the generator's minute-to-second seam loop.

## Milestone 6

- The band panel has no per-signal units, and the cold-key lower line comes from `greatest()` with a NULL median.
- `demo.sh` counts its 15-minute give-up from the submit, not from the start, and prints the refusal on stdout.
