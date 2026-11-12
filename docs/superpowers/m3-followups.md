# Milestone 3 follow-ups

Planning material, like the rest of `docs/superpowers/`. These are review findings from milestone 3 (alert noise control) that were deferred on purpose, grouped by the milestone where each should land. None blocked the merge.

## Milestone 5 (throughput and delay)

- The union of events and ticks adds one local channel hop per event; measure it in the benchmark.
- The fixed-rate benchmark keeps every partition busy, so its delay figures see a watermark lag of about 21 s. The write-up must say that a sparse partition adds up to 60 s.
- `unit-tenants` state removes stale unit/tenant pairs only lazily. Watch state size.
- `ticksDoNotOutrunABacklog` would pass without ticks: end of input closes all three minutes. Collect one more minute so only a tick can close it. Smoke check 10 pins the property end to end meanwhile.
- `watermark-lag.sh` reports a lag over 24 h as "no watermark yet", which is misleading during a history replay.
- **Clock step-back.** The Docker VM's wall clock stepped back about 100 ms once, and Flink refused a MiniCluster task start (`SubTaskInitializationMetrics`: endTs >= startTs). On Colima it follows host sleep, so `make test-java` can flake after a sleep. In a live job it costs a restart, with a freeze of up to 60 s.

- **Checkpoint size in the eval summaries.** The summaries record it, and §6 reads state size from it. With a hashmap backend it is not the size of a RocksDB state, so §6 needs one run on RocksDB instead.
- **Busy times from the eval runs** (Task 15): the `scorer` and `region-grouper` busy times belong in the throughput write-up.
- **Scorer writes.** Check whether the Scorer's unchanged machine-state writes were removed, or are still written every minute (see `m1-followups.md`, milestone 5).
- **`live.*` topics carry event time.** The `live.*` output topics are stamped with event time, so the broker's 7-day retention deletes backfilled rows within hours of the write (seen on the demo stack: `live.alerts` starting at offset 8). The forwarder's rebuild from the start of `live.alerts` then misses them. Give `live.*` `LogAppendTime` too, or say why not.

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
- **Cold-bucket resolve of a seasonal volume alert, as documented behaviour.** A firing seasonal volume alert resolves on entering a cold bucket, because `ZeroTraffic` turns UNSCORED into CALM when a trailing expectation exists. It re-fires in the next warm bucket. It needs a key with warm and cold volume buckets at once. The eval worlds never have one: a key is cold in every bucket for two weeks and warm in every bucket after. Say so in the design doc.
- **For `docs/design.md`, from milestone 4:**
  - the eval world's composition (48 keys, not "about 60", and why);
  - the placement builder's calls (scenario spacing, the eligibility rules, the thresholds mask);
  - the scoring builder's calls (what counts as a page, a match and a false page);
  - the owner's decisions of the plan (expected-signal recall as the selection and headline measure, the mask amendment, the seed protocol).
- **Open items from `m1-followups.md`:** tests for non-JSON webhook bodies, and the script items (the default submit misses jobs in CREATED or INITIALIZING; `make ci` and `ci.yml` are kept in sync by hand; the Flink CLI prints `Unknown module: jdk.compiler` from the image).
- **`check-paths.json` timing.** Its `total` uses the host's monotonic clock, which stops during a host sleep, while the job seconds come from Flink's wall clock. After a sleep, `total` can be smaller than one job's time.
