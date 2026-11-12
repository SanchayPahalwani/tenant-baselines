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
- **`live.*` topics carry event time.** The `live.*` output topics are stamped with event time, so the broker's 7-day retention deletes backfilled rows within hours of the write (seen on the demo stack: `live.alerts` starting at offset 8). The forwarder's rebuild from the start of `live.alerts` then misses them. Give `live.*` `LogAppendTime` too, or say why not.

From the milestone 4 final review:

- **Java Minors in the job (they change the jar).** `ThresholdsFile` treats a non-object `global` or `per_key` node as empty, and reports a non-integer `valid_from_ms` as missing. Its overlap message lacks the `#index` form. One line is 150 characters long. `AggregateJson`'s "not JSON (…)" message has no length bound. The `fin && due > replayEndMs` branch of `Scorer.onTimer` has no test. Milestone 5 or 6.
- **Machine facts per run.** Summaries should record the host and Docker VM facts measured at run time. `docs/results/machine.json` holds them once, for the 15 official runs. The benchmark write-up needs the same.
- **Connector 5.1.0.** Move to it when it is released, and drop the rule that a replay-mode run hitting FLINK-39728 is simply run again.
- `eval_run`'s poll retries can overshoot `timeout_s` by about a minute.
- `submit-job.sh` writes the checkpoint path twice.
- `create-topics.sh --if-not-exists` keeps an old stack's topic configs.

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
  - the owner's decisions of the plan (expected-signal recall as the selection and headline measure, the mask amendment, the seed protocol);
  - the dedicated-unit drop moves only the tenant's dedicated key, not its key on the shared unit;
  - the cold-start burst has no volume rule, so it can land in minutes the seasonal detector does not score (seasonal found 2 of 5 at k6);
  - a regional page matches every scenario with an affected key in its region, tenant-level scenarios included;
  - a page's week is set by `starts_at_ms`, its match by `event_time_ms`;
  - load is per key, not per tenant.
- **Open items from `m1-followups.md`:** tests for non-JSON webhook bodies, and the script items (the default submit misses jobs in CREATED or INITIALIZING; `make ci` and `ci.yml` are kept in sync by hand; the Flink CLI prints `Unknown module: jdk.compiler` from the image).
- **`check-paths.json` timing.** Its `total` uses the host's monotonic clock, which stops during a host sleep, while the job seconds come from Flink's wall clock. After a sleep, `total` can be smaller than one job's time.

From the milestone 4 final review:

- **Measured Python modules, cleanups only.** `shape_array` and the error and latency formulas restate `traffic.py`. The `weeks <= 0` message. In `thresholds.py`: the unused `_MINUTES_PER_WEEK`, the `assert` that `-O` removes, and the `--demo` flag that is always required. In `scenarios.py`: `_MAX_ATTEMPTS` reads as per scenario but counts per week, and `_SCORED_FROM` doubles as the cold-start week. Any edit breaks the report's "measured code equals the freeze commit" line, so batch them with a stated reason, or leave them.
- **A reader cannot reproduce an official run.** `make eval` is sealed, `eval-dev` refuses official seeds, and a rebuilt jar has another hash. Add a `repro` kind: an official seed, a local build, output under `data/eval/repro-*` only, and a comparison of `selected_week5` with the committed summary.
- **False-page breakdown.** A script that splits the selected settings' false pages between bursts, day factors and the rest, so the README can explain them with measured figures. Analysis only; no setting changes.
- **Repo-wide rule test.** Check the banned words and the "synthetic" label over the whole tracked tree, not only the rendered `results.md`.
- **Pages per regional outage.** Spec 4.7 asks for them. The outage table shows whether a regional alert formed; the page count is only pooled (seasonal: 3.00 per detected outage).
- **Test-only leftovers.** `WatermarksTest` pins only the timestamp assigners. `PopulationBaselinesTest` has a 60 s timeout. The rows-order test is O(rows × keys). `checked()` guards continuity per key only.
- **Eval driver and report, small gaps found after the fix wave.**
  - After a bug re-run, the "measured code equals the freeze" line still compares HEAD with the freeze commit, and any `bug:` entry accepts a "no". A later change to measured code would go unflagged.
  - After a bug fix, a failed re-run can only be repeated under `bug:`, not `infrastructure:`, because of the freeze diff and the anchor.
  - With a selection that matches no planned pair, the driver prints "all 0 official runs are finished".
  - A dev render prints the official runs' machine-facts source line, which is wrong for dev runs.
  - `make eval` still starts the stack (`eval: up`), so "it now only renders" needs Docker.
  - `eval_report.py` still defines its own `REPO`; import it from `freeze`.
  - The test resource `thresholds-eval-dev-s7-a6.json` cannot carry a "synthetic" label: the format has no field for one. Its test says so instead.
