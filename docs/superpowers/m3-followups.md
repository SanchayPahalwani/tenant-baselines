# Milestone 3 follow-ups

Planning material, like the rest of `docs/superpowers/`. These are review findings from milestone 3 (alert noise control) that were deferred on purpose, grouped by the milestone where each should land. None blocked the merge.

## Milestone 4 (the eval)

1. **Identical alert logs are a plain diff only for one region.**
   - The grouper is keyed by region and the job runs at parallelism 2, so regions interleave in `<ns>.alerts` in arrival order.
   - Compare sorted by `(event_time, region, detector, setting, signal, key, state)`.
   - Leave out `emitted_at`, which is the wall clock.
2. **Aggregates-mode timestamps.**
   - `RegionGrouper` buffers candidates by their record timestamp and activity flips by `minuteMs`.
   - The aggregates source must assign a timestamp inside each aggregate's minute. Otherwise records carry Kafka produce time and candidates land on wall-clock timers, with no error.
   - Add a test that a candidate's timer equals the minute end.
3. **Zero-count rows.** The aggregate generator must emit zero-count rows exactly as `MinuteAggregator` does, from a key's first event to expiry or replay end. Otherwise `KeyActivity` keeps a silent key active.
4. **`Scorer`'s expiry at the final watermark** (milestone 2 code: the early return at `Long.MAX_VALUE`, around `Scorer.java:249`).
   - An expiry due after the last real watermark but before the replay end is skipped, so an `expired` resolve can appear in one mode's log only.
   - Fix: expire at the final watermark when the due time is at or before `replayEndMs`.
5. **The end of a replay.**
   - Held alerts are released, and a streak in progress resolves, with `event_time` up to a few minutes past the replay end. This is deterministic.
   - A regional alert at or above 25% stays open with no resolve.
   - Scoring clips at the replay end and treats open alerts explicitly.
6. **Timestamps for scoring.**
   - A tenant `firing` row's `event_time` is the hold end, and `starts_at` is the first breaching minute.
   - A regional row's `starts_at` is the promotion minute.
   - Measure delay with `event_time` for both scopes.
7. **`/` in a detector name** breaks `Group.of` in `RegionGrouper` and the Scorer's id split; a `/` in a setting is fine. Reject it in `ThresholdsFile`, together with the existing `ThresholdsFile` item in `m1-followups.md`.
8. **Unit status follows a different setting from the pager.** It follows the first `--seasonal-k`, while the forwarder pages `--live-setting` (k4). With `--seasonal-k 3,4,5,6`, drains would follow k3. Align them when the eval runs several seasonal settings.
9. **`AlertEvent` is now checkpointed state** (the grouper's `pending-alerts`), so changing its fields is a state-compatibility change.
10. **Cost.** `alertsByGroup()` scans every alert of a region on every timer, and a firing regional alert means a timer every minute. Measure it on a dev seed before the official runs, with 12 settings.
11. **Tests to add when the code is next touched:**
    - `PopulationLookupTest`: the week-1 parity-slot case.
    - `RegionGrouper`: the step-2 sort, a -1 activity flip down to zero, two active keys of one tenant in the denominator, and `KeyActivity`'s out-of-order guard.
    - These may also be covered by `make check-paths` disagreeing.
12. **A tenant alert absorbed by two regional alerts** writes two `absorbed` events with one `(alert_id, state)`, and `tb.alert_log` keeps the later one. The scoring must expect that.

## Milestone 5 (throughput and delay)

- The union of events and ticks adds one local channel hop per event; measure it in the benchmark.
- The fixed-rate benchmark keeps every partition busy, so its delay figures see a watermark lag of about 21 s. The write-up must say that a sparse partition adds up to 60 s.
- `unit-tenants` state removes stale unit/tenant pairs only lazily. Watch state size.
- `ticksDoNotOutrunABacklog` would pass without ticks: end of input closes all three minutes. Collect one more minute so only a tick can close it. Smoke check 10 pins the property end to end meanwhile.
- `watermark-lag.sh` reports a lag over 24 h as "no watermark yet", which is misleading during a history replay.
- **Clock step-back.** The Docker VM's wall clock stepped back about 100 ms once, and Flink refused a MiniCluster task start (`SubTaskInitializationMetrics`: endTs >= startTs). On Colima it follows host sleep, so `make test-java` can flake after a sleep. In a live job it costs a restart, with a freeze of up to 60 s.

## Milestone 6 (README and design doc)

- **Live watermark, for `docs/design.md`:**
  - Ticks use the TaskManager's clock; events use the producers' clocks. After a silence, producers more than about 20 s behind the TaskManager lose their first events.
  - A forward step of the TaskManager's clock while partitions are idle closes minutes early.
  - Producers that buffer for more than 60 s and then flush lose that data as late, while the job alerts on the silence.
  - The startup exposure is guarded only by `make demo`'s wait for every partition. A bare `submit-job.sh` on empty partitions drops history written later; `lateEvents` is the only sign.
  - Idleness is a wall-clock guess: a partition with an unread backlog that delivers nothing for 60 s after a restore is left out, and its tail is dropped.
- **Unit-status determinism edges** (owner-visible ruling in milestone 3):
  - The unbuffered `unit-tenants` write lets a tenant whose first request falls between a timer and the watermark count at that timer.
  - At a replay's tail, with another setting's regional alert at streak 0, a drain made possible by an expiry moves to the region's next input timer.
  - Both affect only `tb.unit_status` rows, never an alert or a page.
- A tenant alert absorbed by two regional alerts writes two `absorbed` events with one `(alert_id, state)`, and `tb.alert_log` keeps the later one. Say so in the design doc.
- **Unit status:** a drained unit keeps the signal and reason it was first drained for, until it undrains. Add a test, and say so with the unit-status panel.
- `RegionGrouper.java` is about 670 lines. Unit status is separable; extract it when the file is next touched.
- No end-to-end run has seen a regional alert resolve (check 12 stops the generator first). Consider a smoke check, without lengthening the run much.
- `smoke.sh`'s `check_inhibit` prints a Python traceback on every poll while Alertmanager is down.
