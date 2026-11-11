# Milestone 2 follow-ups

Planning material, like the rest of `docs/superpowers/`. These are review findings from milestone 2 that were deferred on purpose, grouped by the milestone where each should land. None blocked the merge.

## For the owner, at the start of milestone 3

- **Watermark limitation.** If the job restores, or a task fails over, while every partition is silent, the watermark stays put until the first event arrives. The zero-traffic rule cannot page for that outage.
- **Hand-over gap in the latch.** The latch can hand over to the wall clock while a split still has unread events between `now - 60 s` and `now - 20 s`. At demo rates this is invisible (`lateEvents` is 0 across the seam). At benchmark rates that tail would be dropped as late and look like a volume drop.
- **Proposed redesign.** It needs an owner decision and a spec amendment:
  - a Kafka source with plain per-split event time, `forBoundedOutOfOrderness(20 s).withIdleness(60 s)`;
  - a 1/s tick stream from `DataGeneratorSource` (`flink-connector-datagen`), stamped with the wall clock and a 20 s bound;
  - `union` the two and filter the ticks out before the minute aggregator.
- Settle this no later than before milestone 5 publishes delay numbers.

## Milestone 3

- The smoke restore check cannot fail on its stated property: `count() - uniqExact(dedup key)` over `FINAL` is 0 by construction for one run. Compare against `uniqExact(detector, setting, key, signal, starts_at, state)`.
- `Scorer.java` is large. Split out the lookup helpers and `alert()`.
- Add a regression test for `finiteOrZero`.
- The forwarder's argparse defaults are untested. Extract `build_parser`.

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

- Stale week-4 histogram entries leak in `PopulationBaselines` when a bucket's hour gets no scored minute for a week. Sweep weeks below `w - 4` on each timer.
- No test drives the generator's minute-to-second seam loop.

## Milestone 6

- The band panel has no per-signal units, and the cold-key lower line comes from `greatest()` with a NULL median.
- `demo.sh` counts its 15-minute give-up from the submit, not from the start, and prints the refusal on stdout.
