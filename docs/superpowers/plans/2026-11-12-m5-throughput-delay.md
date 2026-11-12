# tenant-baselines Milestone 5 Implementation Plan: throughput and delay

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `make bench` measures three things on the laptop stack and writes them down: how many events a second the live Flink job sustains on a pre-filled topic of 2,000 keys; how long an alert takes from the moment it is decidable, and from the incident's start, to its append on the alerts topic (p50 and p95); and how much checkpointed state 48 keys with a real 4-week history hold. A script renders `docs/performance.md` from the result files.

**Architecture:** Three Python drivers, one per measurement, share a few helpers: a preflight (AC power, VM clock, no other job), an environment record, fresh reads of Flink's REST metrics and checkpoint statistics, and the VM clock's offset from the host. Each driver writes one JSON result file under `docs/performance/`; a report script renders the Markdown from those files and from nothing else. The Flink job is not changed: the benchmark runs the jar that produced `docs/results.md`.

**Tech stack:** unchanged. No new dependency.

**Spec:** `docs/superpowers/specs/2026-11-09-tenant-baselines-design.md` (v2). Milestone 5 is §8 item 5 and §6 is its authority. Read §3, §4.1, §4.2, §4.5, §4.7, §4.9, §6, §7, §8 and §10 before starting. Follow-ups folded in or deferred come from the "Milestone 5" sections of `docs/superpowers/m1-followups.md`, `m2-followups.md` and `m3-followups.md`. If `docs/superpowers/m4-followups.md` exists when this plan is built, read its "Milestone 5" section and apply the rule under "Follow-ups" below.

**Budget:** 6.5–7.5 hours, against §8's "~3 h". About 4.5 h is building and reviewing. About 2.2 h is machine time: the throughput pilot and its two runs about 0.5 h, the delay pilot about 0.35 h, the delay run in the sitting about 1.3 h, state about 0.1 h. The rest is contingency. **The spec's "~3 h" cannot hold:** the delay run alone is about 80 minutes of wall time, §8's figure did not count the follow-ups from three reviews or the run-condition guards, and the milestone 4 plan ran at about three times its §8 figure for the same reasons.

**Precondition:** milestone 4 is merged to `main`, and `docs/results/official-runs.jsonl` holds a `finished` entry for all 15 `(seed, amplitude)` pairs. `eval_official.preflight` compares the whole tree with the freeze commit, so the first commit of this milestone ends the period in which an official run can start without `--rerun-reason 'bug: ...'`. Work on a branch `m5-bench` cut from `main`.

**API checks done for this plan (2026-11-12, `ctx7`, no quota failures):**
- Flink `/apache/flink` at `release-2.2.0`:
  - `GET /jobs/:jobid/checkpoints` returns `counts`, `latest.completed` and a `history` array; each history entry has `id`, `status` (`IN_PROGRESS`, `COMPLETED`, `FAILED`), `trigger_timestamp`, `state_size` and `checkpointed_size`.
  - `GET /jobs/:jobid/checkpoints/details/:checkpointid` returns one checkpoint with a `tasks` object per vertex.
  - Metrics are read per vertex at `/jobs/<jobid>/vertices/<vertexid>/metrics?get=…`, as `scripts/flink_metrics.py` already does.
- **Not confirmed by ctx7 or by code in the repo; Task 2 Step 6 reads each on the running stack before a driver depends on it:**
  - the task-level ids `0.numRecordsOut` and `1.numRecordsOut` on `Source: events` (beside operator-level ids such as `0.Source__events.numRecordsOut`);
  - per-vertex `state_size` and `checkpointed_size` under `tasks` in a checkpoint's details;
  - `history[].status`, `trigger_timestamp` and `checkpointed_size` of `/jobs/<id>/checkpoints`;
  - `start-time` and `end-time` of `/jobs/<id>` and of its `vertices[]`;
  - `interval` and `state_backend` of `/jobs/<id>/checkpoints/config`;
  - `flink-version` of `/config`;
  - `date +%s.%N` inside the jobmanager image;
  - confluent-kafka's `Message.timestamp()` returning `(type, ms)` with `TIMESTAMP_LOG_APPEND_TIME` (no match in `/confluentinc/confluent-kafka-python`).
- Confirmed by code already in the repo: `exceptionHistory.entries`, `accumulated-busy-time`, `accumulated-backpressured-time`, `accumulated-idle-time`, `duration`, `vertices[].name` (`eval_run.py`); `.minute-aggregator.currentInputWatermark` (`scripts/watermark-lag.sh`); `.late-events.lateEvents` (`scripts/late-events.sh`).
- Read from the code and the files on disk (2026-11-12, nothing was run):
  - The events source starts at `OffsetsInitializer.committedOffsets(EARLIEST)`. A group id that has never committed therefore starts at the earliest offset; the job needs no start-offset flag.
  - `Watermarks.replay()` aligns splits with a maximum drift of 1 minute; live mode has no alignment. `docs/results/check-paths.json` records 25,780,861 events through a replay-mode job in 3,026 s, about 8,500 a second: the alignment is the cap, not the job.
  - Six dev eval runs (`data/eval/dev-*/summary.json`): 2,358,720 aggregate rows in 101.3–132.0 s, `scorer` busy 173.9–230.8 s across 2 subtasks. Their `checkpoint_state_size` is the *latest completed* checkpoint of a job that checkpoints every 30 s.
  - The demo generator wrote 24,338,688 history events in 119 s (README), about 200,000 a second. The pre-fill uses the same per-event `encode` and `produce` path.
  - A streak starts at the first breaching minute and fires on the third (`AlertStateMachine.FIRE_AFTER = 3`). The hold is `RegionGrouper.HOLD_MS = 120_000` from that minute's end, and a tenant `firing` row's `event_time_ms` is the hold timer + 1 ms. So the row is stamped 300 s after the *start of the first breaching minute*.
  - `infra/flink/thresholds-demo.json` has two sets, `global_static/p99.9` and `per_key_static/p99.9`, valid from 0 with no end. The global levels fire `error_rate` at 0.077 and `latency_p95` at 394.9 ms; `per_key` lists only the demo's keys.

## Global Constraints

- Everything in the milestone 1–4 plans' Global Constraints still applies.
- Java package root `io.github.sanchaypahalwani.tenantbaselines` (written `<pkg>`). Java builds and tests run only through `make` (`make test-java`, `make jar`); there is no host JDK. Python: `make test-py && make lint-py`.
- **No job code changes.** Nothing under `flink-job/src/main` is edited. The only Java change is one test (`TicksTest`, Task 1). HEAD's jar stays the code that produced `docs/results.md`.
- **The eval's Python is not edited:** `evalmodel.py`, `scenarios.py`, `simulate.py`, `thresholds.py`, `scoring.py`, `replay_events.py`, `eval_run.py`, `eval_official.py`, `eval_report.py`, `check_paths.py`, `freeze.py`, `kafka_io.py`. `world.py` is not edited either: it is one of `freeze.MEASURED_PATHS`, and the `docs/results.md` render refuses once a measured path differs from the freeze commit. The two new worlds live in a new module, `bench_world.py`, built from `world.py`'s dataclasses; `gen_live.py` sends `--world bench` and `--world delay` there and every other name to `build_world`. Before every commit, `git diff --quiet <eval/freeze.json's commit> HEAD -- <freeze.MEASURED_PATHS>` exits 0; `test_bench_world.py::test_measured_paths_equal_the_freeze` runs it and skips where the git history is absent. `gen_live.py` gets a new world, a new script and `take_step`; the `--script demo` and `--script none` branches keep their behaviour, and the producer's settings change only under `--script bench`. Every existing test stays green unchanged. A bug re-run of an official seed must still simulate the same world and read Kafka through the same code.
- **Every number in `docs/performance.md` comes from a result file under `docs/performance/`, written by a driver.** The report script computes nothing from memory and takes no number from this plan. Design constants (the 20 s watermark bound, the 60 s idleness, 3 minutes to fire, the 120 s hold, Alertmanager's 30 s `group_wait`) sit in a `design` block of the result file and are printed as "configured, not measured". An estimate is labelled an estimate; the extrapolation is labelled an extrapolation. The data is synthetic and every section says so. No claim of production use.
- **Run conditions.** A run needs the Mac on AC power with the lid open throughout: a lid-close sleep freezes the Docker VM. Each driver runs `pmset -g ps` at its start and refuses unless the first line says `AC Power`, or `--on-battery` is passed, as the eval driver does; a run on battery records that. Before a long step, the person at the keyboard runs `pmset -g ps` too and reads it. Trust the reading, not a claim. `KEEP_AWAKE=1` wraps a driver in `caffeinate -is`, as `make eval` does; it does not stop a lid-close sleep. Close the Flink web UI during a run (see "fresh reads" below).
- **A run is valid only if** the job never restarted (its exception history is empty), `lateEvents` is 0, and no two polls of the driver's loop were more than 60 s apart on the host's monotonic clock (a host sleep). The delay run also needs the VM clock's offset from the host to move by no more than 1 s between its start and its end. A driver writes `"valid": false` with the reasons otherwise, and the report script refuses an invalid file.
- **The Colima VM is 8 CPUs / 10 GiB and is shared.** Do not restart or resize it. Other projects' containers may be running in it; the environment record stores how many, never their names. Run `colima status` first; if the VM is stopped, start it with `colima start --cpu 8 --memory 10` (the spec's size, §3 and §10 item 11) and say so in the report. When the milestone's runs are done, stop this project's containers with `make down`; stop Colima only if `docker ps` then shows nothing.
- **Topics are never deleted, and a run id is never reused.** Each bench run has its own input topic and its own consumer group. Output goes to the shared `bench.alerts`, `bench.aggregates`, `bench.scores` and `bench.unit-status`, and is told apart by `run_id`.
- **Fresh reads.** Flink's REST API answers metrics from a cache that refreshes only when a request finds it more than 10 s old (`scripts/flink_metrics.py`). A sample is one burst: a request that starts the refresh, a 1 s wait, then every read of that sample within the next few seconds. Between two samples a driver makes no REST call. Anything else that asks for metrics (the web UI, `watermark-lag.sh` run by hand) moves the refresh and makes a sample's timestamp newer than its value.
- **Output JSON never contains `NaN` or `Infinity`.** Result files are written with `allow_nan=False`, sorted keys, and a `"data": "synthetic"` field.
- **Repo rules:** name no employers, clients, internal systems or companies; Apache-2.0; none of the words spearheaded, leveraged, synergy, cutting-edge, results-driven, passionate; plain, short sentences; commit locally after each task, never push, never add a remote, never create a GitHub repo.

## Decisions

**Controller rulings (2026-11-12, after the plan's review; builder's calls):**
1. Time-to-page is measured and stated as measured. The write-up explains the 3-minute confirmation and the 2-minute hold; it has no fixed "at least about 5 minutes" sentence. Tests and the pilot's expected range follow the code: about 270–330 s.
2. The VM clock's offset from the host is measured to under a second and subtracted from every watermark-lag sample.
3. The bench world's unit latencies are 30–80 ms, under the demo file's static latency level, and each throughput run records its alert rows.
4. The `--ticks` switch and the four-run comparison are dropped. The union hop is measured from the `drop-ticks` vertex's busy time in two normal runs and reported as an upper bound on its CPU cost. The `TicksTest` fix stays, first, as its own commit.
5. The README gets a link to `docs/performance.md` and no numbers; a numbers block is milestone 6's.
6. Validity keeps the battery refusal, the no-other-job check, restarts, `lateEvents`, the clock offset and the host-sleep check. The lid reading, the dirty-tree refusal at publish time and the mixed-commit refusal are dropped.
7. The delay run settles for 5 minutes and has a 3-minute tail. It keeps 60 incidents, for the p95, and keeps its pilot.
8. The state run publishes directly; there is no hashmap out-of-memory path.
9. Seven tasks: each world is built with its only user, and the docs are written with the report.
10. Budget 6.5–7.5 h.
11. **The eval's measured paths stay byte-identical (after milestone 4 merged).** The `bench` and `delay` worlds go in a new `bench_world.py`, not `world.py`, because `world.py` is in `freeze.MEASURED_PATHS` and the `docs/results.md` render refuses a measured path that differs from the freeze commit. `eval_run._container_path` is now `freeze.container_path`.

**Builder's calls on the throughput run** (each with its reason):
- Input topic `<fill_id>.events`, not `bench.events`. *A second pre-fill on the same day would sit behind the first in `bench.events` (retention is one day), and a job from the earliest offset would read both.*
- World `bench`: 3 regions, 1,000 tenants, 24 shared units, 2,000 keys, flat traffic. *§6 asks for one cardinality of about 2 K keys; flat traffic keeps the event density the same in every minute of the fill.*
- Unit base latencies uniform in 30–80 ms. *Events are drawn around base × 1.6 with sigma 0.5, so a unit's p95 is base × 3.64: at most 291 ms, under the demo file's 394.9 ms. At the eval world's 60–120 ms about a fifth of the units would hold a static latency alert for the whole run.*
- Density: 120 requests a minute per key for 150 minutes of event time, about 36 million events, the last stamped just before the fill starts. *240,000 events per event minute is dense enough that the job's own speed, not the passing of event time, sets the pace; 150 minutes close about 300,000 key-minutes, so detection is in the measured path; stamps that end at the wall clock keep the live watermark from jumping a long gap when the backlog ends.* `BENCH_MINUTES` is capped at 300.
- Mode `live`. *It is the job the demo runs, and replay mode's 1-minute alignment caps a backlog read. While a live job is behind, its watermark is pure event time (§4.2).*
- The job runs as the live job does: seasonal `k4`, the demo thresholds file, `--scores true`, parallelism 2, RocksDB, incremental checkpoints every 30 s.
- **The metric:** the task-level counter `numRecordsOut` of the vertex `Source: events`, summed over its subtasks, sampled every 12 s. The sustained rate is the counter's rise divided by the time between two samples: the first sample at least 60 s after the counter first moved, and the last sample below 90% of the events filled. The window must be at least 120 s and 8 intervals long, or the run fails. *A counter difference has no averaging window of its own, unlike `numRecordsOutPerSecond`. The first 60 s hold the job's start, the first RocksDB flushes and the JIT. Live mode does not align partitions, so one source subtask can run dry before the other; stopping at 90% keeps that tail out.* Each sample can be up to about 1 s off; the result file carries `timing_error_pct = 100 × 2 × 1 s ÷ window`, and the write-up prints it.
- **The counter must end exactly at the number of events filled.** *If it stops short or overshoots, the id is not the task-level record count, and the rate is wrong by the same factor.*
- **That it measures Flink:** the driver checks that the topic holds exactly the events produced before it submits, and no producer runs during the job. The write-up gives the generator's own fill rate beside the job's rate, and the shares of the window the source spent back-pressured and idle. *A source that is neither busy nor back-pressured is waiting on the broker; the rate is then not the job's, and the write-up must be able to show that.*
- **Two runs on the same fill.** *They give the run-to-run spread. The first job on a TaskManager is the colder one; both are printed.*
- **The union hop (`m3-followups`):** the `drop-ticks` vertex's busy time over the rate window, as a share of `parallelism × window`. The write-up calls it an upper bound on the hop's CPU cost. *The task also serializes each event for the keyed shuffle, which the source would do itself without the union. Measuring the difference in throughput would need a job switch and four runs; this needs neither.*

**Builder's calls on the delay run:**
- World `delay`: 3 regions, 36 tenants with one key each, 40 requests a minute per key, flat. *40 a minute keeps every minute above minimum evidence (20); 36 keys keep every one of the 8 partitions busy, which is the fixed-rate case the write-up describes.*
- One continuous timeline, as the demo: 21 days of history at full speed, then live. *The seasonal detector needs 2 weeks of a key's own history, and the live setting, seasonal `k4`, is the headline. The static sets fire on the same incidents and are reported beside it.*
- The generator runs in the compose `generator` image, not on the host. *Event time comes from the generator's clock and append time from the broker's. In one VM they share a clock.*
- Incidents: 60, each an error spike of +20 points on one key for 6 minutes. In each region one starts every 3 minutes, on the region's 12 keys in turn; the regions are offset by 60 s; each start is moved by a seeded 0–59 s. The first starts 5 minutes after the generator catches up. *+20 points scores about 22 against a spread of about 0.9 points at 40 requests; a random second covers every phase of the minute; at most 4 of a region's 12 tenants can hold an alert at once (33%), below the 50% that forms a regional alert.*
- Labels: the generator writes `labels.json` when it catches up: each incident's id, key, region, signal `error_rate`, start and end in event-time milliseconds.
- Matching, the eval's rule: an incident is matched by the earliest `firing` row of scope `tenant` on its key and on `error_rate`, for the detector and setting in question, with `start ≤ event_time ≤ end + 10 min`.
- Append time: the broker timestamp of the record in `bench.alerts`, read with the record. The read fails unless the timestamp type is `LogAppendTime`. For an `(alert_id, state)` written twice, the earliest append counts (`m1-followups`). The driver reads Kafka, not ClickHouse.
- Pipeline delay = append time − the row's `event_time_ms`. Time-to-page = append time − the incident's `start_ms`. Percentiles are nearest-rank (`scoring.nearest_rank`), reported with their `n`.
- **What time-to-page is expected to be.** 300 s from the start of the first breaching minute, plus the pipeline delay. A spike this large breaches in the minute it starts in when about 11 s or more of that minute is covered (seasonal), so the count usually starts before the incident does. With the start at second *s* of its minute, time-to-page is about 321 − *s* seconds, or a minute more when *s* is in the last ten seconds: about 262–332 s, with a p50 near 300 s and close to half of the incidents under 5 minutes. These are expectations for the pilot, not text for the write-up.
- **The measurement ends at the alerts topic.** The forwarder's poll and Alertmanager's `group_wait` come after it and are not in either figure. The README's milestone 3 page times were measured at the webhook, from the start minute; the write-up says the two are not comparable and does not retype them.
- **Watermark lag** is sampled once a minute from the first incident's start: `(host request time + VM clock offset) − lowest watermark`. The offset is `date +%s.%N` in the jobmanager, bracketed by two host readings, the tightest of five tries; it is read before and after the run.
- A run with fewer than 90% of its incidents matched is invalid.

**Builder's calls on state size:**
- The eval world has **48 keys**, not §6's 60 (milestone 4's builder's call on §5.2); 46 run for five weeks and 2 from week 4. The measurement uses it as it is and says 48.
- A dev seed (7, amplitude 6) through an aggregates-mode job with the eval's settings, but with `-Dexecution.checkpointing.interval=5s`. *The job is bounded and ends about 105 s after it starts; at the cluster's 30 s interval the last periodic checkpoint can be a week and a half of event time before the end of week 5.*
- The checkpoint reported is the latest completed one triggered before the source vertex finished. It must fall in the last 15% of the time from the job's start to the source's end, or the run fails. *A checkpoint taken after tasks finished may hold less. Over five weeks the state only grows: the baseline ring has 5 weekly slots and nothing is overwritten before week 6.*
- Two runs. **Hashmap (`-Dstate.backend.type=hashmap`) is the per-key headline:** a full snapshot of serialized state. RocksDB with incremental checkpoints, the stack's backend, is reported beside it, with the range of `state_size` over the checkpoints of the last 15%. *At a 5 s interval RocksDB's `state_size` counts SST files not yet compacted, so it moves from one checkpoint to the next.*
- Each run's backend and interval are read from `/jobs/<id>/checkpoints/config`, not from the cluster's defaults.
- Per key = the `scorer` vertex's size ÷ 48. The other vertices are reported as they are.
- Aggregates mode has no `MinuteAggregator`. Its state for 2,000 keys comes from the throughput run's last checkpoint, and is reported there.
- **Extrapolation:** the scorer's per-key bytes × 2,000, 20,000 and 100,000 keys, computed by the report script and printed under a heading that says "Extrapolation, not measured". It leaves out `region-grouper` (keyed by region, holding per-key activity), `MinuteAggregator` (per key, events mode only) and `population-baselines` (does not grow with keys), and the write-up says so. §6 puts it in `docs/design.md`, which milestone 6 writes; milestone 6 links or moves the table.
- The number of settings each job ran is counted, not typed: the seasonal ks in `eval/freeze.json` plus the distinct `(detector, setting)` pairs in the run's thresholds file; for the live job, one plus the pairs in `thresholds-demo.json`.

## Follow-ups: folded in or deferred

| Source | Item | Decision |
|---|---|---|
| m1 | Pipeline delay from the first emit, not `FINAL` | **Folded** (Task 4): earliest append per `(alert_id, state)`, read from Kafka. |
| m1 | Delete retained checkpoints of cancelled jobs | **Folded for bench jobs** (Task 2): a driver deletes its run's checkpoint directory after the cancel. **Deferred for the live job** to milestone 6's compose pass: `submit-job.sh` restores from those directories, and a change there needs `make smoke`. |
| m1 | TaskManager memory (2560m) at benchmark state sizes | **Folded as a measurement** (Task 3): the container's memory after each run. Only the first reading describes one job; the second is cumulative in one JVM, and the write-up says so. Kept at 2560m unless a run restarted or the container was OOM-killed. |
| m1 | Scorer writes machine state every minute | **Folded as a measurement** (Task 3): busy and back-pressured time per vertex. No optimisation here. |
| m1 | Generator delivery failures surface after 300 s | **Folded for the bench script** (Task 4): `delivery.timeout.ms` 60 s and an exit on a failed delivery, under `--script bench` only. The demo's producer is unchanged; that stays on the milestone 6 list. |
| m2 | `PopulationBaselines` keeps stale week-4 histogram entries | **Deferred** to the next change of that operator (milestone 6 list). *It needs a bucket hour with no scored minute for a week, which no bench or eval world has; and this milestone does not edit job code.* |
| m2 | No test drives the generator's minute-to-second seam | **Folded** (Task 4): the delay run crosses that seam. |
| m3 | Union hop of events and ticks | **Folded as a measurement** (Task 3): the `drop-ticks` vertex's busy share, an upper bound. |
| m3 | 21 s lag at a fixed rate; a sparse partition adds up to 60 s | **Folded** (Tasks 5, 7): the lag is measured during the delay run and printed from the file; the 60 s is the configured idleness and is printed as configuration. |
| m3 | `unit-tenants` removes stale pairs lazily | **Folded as a measurement** (Tasks 3, 6): `region-grouper`'s checkpointed size at 2,000 keys and after five weeks. No bench world makes a pair stale, and the write-up says that. The fix stays on the milestone 6 list. |
| m3 | `ticksDoNotOutrunABacklog` passes without ticks | **Folded** (Task 1). |
| m3 | `watermark-lag.sh` calls a lag over 24 h "no watermark yet" | **Folded** (Task 5). Its lag is against the host's clock; the header comment says so, and the offset correction for the script goes on the milestone 6 list. |
| m3 | Clock step-back after host sleep | **Folded as a guard and a note** (Tasks 2, 7): a restarted job invalidates a run, the VM clock is checked, and the write-up names the cause. The `make test-java` flake is **deferred** to the README's troubleshooting notes in milestone 6: it is Flink's own check. |
| m4 plan | `live.*` topics lose old rows to event-time retention | **Deferred** to milestone 6's compose pass. *Changing the live topics needs `make reset` and `make smoke`, which milestone 6 runs anyway.* |

Rule for an item not in this table: fold it in if a driver in this plan already touches the code and a number depends on it; otherwise move it to the milestone 6 list in Task 7, with one line of reason.

## Where the spec and the code disagree

1. §6 says "60 keys"; the eval world has 48.
2. §6 says "pre-fill `bench.events`"; this plan fills a topic per run.
3. §6 reads state "from checkpoint statistics after week 5 of an eval run"; an eval run's own statistics hold only the latest checkpoint at a 30 s interval.
4. §6 puts the extrapolation in `design.md`, which does not exist until milestone 6.
5. §6 calls the second delay figure "time-to-page"; what the benchmark can stamp is the append to the alerts topic, before the forwarder and Alertmanager.
6. `docs/results/check-paths.json` records `events_job` 3,026.2 s and `aggregates_job` 3,993.4 s but `total` 2,378.7 s, and 443,520 aggregate rows in 3,993 s where a dev eval run does 2,358,720 in under 135 s. The job seconds come from Flink's `duration`, the total from the host's monotonic clock. This plan uses neither. It goes on the milestone 6 list as a question for `results.md`'s method notes.
7. §6 says time-to-page is "at least about 5 minutes for every detector". The code counts the 3 minutes and the hold from the start of the first breaching minute, not from the incident's start, so an incident that breaches in its start minute pages in under 5 minutes of its own start. The write-up states the design as the code has it and prints the measured minimum.

## Review Focus

1. **The Mac sleeps, or runs on battery, in the middle of a run.** Expected: the driver refuses to start on battery without `--on-battery`; a gap of more than 60 s between two polls marks the run invalid; the report script refuses an invalid file. Pinned in Task 2 (`test_preflight_refuses_battery`, `test_gaps_finds_a_host_sleep`) and Task 7 (`test_report_refuses_an_invalid_result`).
2. **The pre-fill is too small for the machine.** A faster job drains it before the rate window is 120 s long. Expected: `WindowTooShort`, naming `BENCH_MINUTES`, and no result file. Pinned in Task 3 (`test_window_too_short_raises`).
3. **An incident with no alert, an alert with no incident, or a regional alert.** Expected: each is counted and shown; percentiles cover matched incidents only and carry their `n`; under 90% matched the run is invalid. Pinned in Task 4 (`test_measure_counts_misses_unmatched_and_regional`, `test_run_is_invalid_under_ninety_percent`).
4. **A stack whose `bench.alerts` was created before append-time stamps.** `create-topics.sh` leaves an existing topic as it is. Expected: the read raises and names `make reset`. Pinned in Task 2 (`test_read_stamped_rejects_create_time`).
5. **Something else is on the cluster, the clocks disagree, or a job restarts.** Expected: the preflight refuses while any job is running; a counter that goes down, or that does not end at the fill's count, raises; duplicates collapse to the first append; a clock offset that moved more than 1 s invalidates the delay run. Pinned in Task 2 (`test_preflight_refuses_a_running_job`, `test_clock_offset_takes_the_tightest_bracket`), Task 3 (`test_counter_going_backwards_raises`, `test_final_count_must_equal_the_fill`) and Task 4 (`test_first_appends_keeps_the_earliest`).

## File map

```
flink-job/src/test/java/<pkg>/time/TicksTest.java                    (Task 1)
py/src/tenant_baselines/
  bench_env.py (new, Task 2)           preflight, environment record, clock offset, gaps, stamped read, result files
  bench_flink.py (new, Task 2)         submit, samples, checkpoint statistics, cancel and clean
  bench_world.py (new, Tasks 3, 4)    worlds "bench" (Task 3) and "delay" (Task 4)
  bench_fill.py (new, Task 3)          the dense pre-fill's events
  bench_throughput.py (new, Task 3)    sustained_rate, window shares, the driver
  bench_delay.py (new, Task 4)         schedule, labels, matching, measures, design constants
  gen_live.py (modify, Task 4)         --world delay, --script bench, take_step
  bench_delay_run.py (new, Task 5)     the delay driver
  bench_state.py (new, Task 6)         the state driver
  bench_report.py (new, Task 7)        renders docs/performance.md
py/tests/test_<module>.py              one per new module; test_bench_world.py; additions to test_gen_live.py
scripts/submit-bench.sh (new, Task 2)  scripts/watermark-lag.sh (modify, Task 5)
Makefile                               bench-throughput (Task 3), bench-delay (Task 5), bench-state (Task 6), performance, bench (Task 7)
docs/performance/{throughput,delay,state}.json  docs/performance.md  README.md
docs/superpowers/m{1,2,3}-followups.md                               (Task 7)
data/bench/<run_id>/                   git-ignored: labels.json, result.json, samples
```

**Implementer and reviewer levels.** Every task: standard implementer. Stronger reviewer for Task 2 (its reads feed every number), Task 3 (the rate window), Task 4 (the matching and the two delay definitions), Task 5 throughout (the timeline and the clocks: the riskiest logic with the fewest tests) and Task 6 (which checkpoint, and what is divided by what). Standard reviewer for Task 1 (one test) and Task 7, whose reviewer checks every sentence that carries a number against the JSON key it reads. The whole-branch review at the end is the stronger one.

**Run-time estimates** (estimates, replaced by the pilots):
- Pre-fill: 36 million events at about 200,000 a second, about 3 minutes.
- One throughput run: unknown; at 100,000–300,000 events a second, 2–6 minutes.
- Delay run: a history of 21 days × 36 keys × 40 a minute = 43.5 million events, about 4 minutes; the job catches up about 2 minutes later; 5 minutes of settling; 60 incidents starting over 60 minutes, the last ending 6 minutes later; a 3-minute tail. About 80 minutes. The pilot with 6 incidents: about 21 minutes.
- State: two runs of about 2.5 minutes each.
- Disk: unknown until the pilot measures the `kafka-data` volume's growth per event. Topics keep a day and are never deleted, so the pilot, the sitting and two delay histories add up.

---

### Task 1: A tick test that needs the ticks

**Implementer:** standard. **Reviewer:** standard.

**Files:** Test: `flink-job/src/test/java/<pkg>/time/TicksTest.java`

- [ ] **Step 1: Change `ticksDoNotOutrunABacklog`.** Collect **4** minutes, not 3. Expect the three minutes of 60 and then `tuple(MON + 3 * MINUTE_MS, 0L)`. Update the Javadoc: the events source ends, so its final watermark closes the three full minutes with or without ticks; the fourth, empty minute is closed only by a watermark that keeps advancing after the input ends, and the aggregator does not re-register at `Long.MAX_VALUE`, so only a tick can close it.
- [ ] **Step 2: Run it.** `make test-java` → 0 failures. The test passes at once: it pins.
- [ ] **Step 3: Show it has teeth.** In `TicksTest.firstMinutes`, replace `Ticks.withTicks(events, ticks)` with `events` locally and run `make test-java`: `ticksDoNotOutrunABacklog` now fails (three minutes collected). Restore the line and run `make test-java` again → 0 failures.
- [ ] **Step 4: Commit.** `git add flink-job/src/test && git commit -m "Make the backlog tick test need the ticks"`

### Task 2: Bench plumbing: preflight, environment, clock offset, Flink reads, submit script

**Implementer:** standard. **Reviewer:** stronger.

**Files:**
- Create: `py/src/tenant_baselines/bench_env.py`, `py/src/tenant_baselines/bench_flink.py`, `scripts/submit-bench.sh`, `py/tests/test_bench_env.py`, `py/tests/test_bench_flink.py`

**Interfaces (produces):**
- `scripts/submit-bench.sh <run_id> <input_topic> [extra flink option...]`, run inside the jobmanager like `submit-eval.sh`, with the same check on the run id and the same "job id alone on the last line of stdout" contract. It runs
  `flink run -d -Dexecution.checkpointing.dir=file:///flink-data/checkpoints/bench/<run_id> "$@" $JAR --bootstrap kafka:19092 --ns bench --run-id <run_id> --mode live --input-topic <input_topic> --thresholds /opt/tb-config/thresholds-demo.json --seasonal-k 4 --live-setting k4 --scores true --group-id tb-<run_id>`,
  and refuses when `/flink-data/checkpoints/bench/<run_id>` exists.
- `bench_env`:
  - `PreflightFailed` is `eval_official.PreflightFailed`, re-exported.
  - `preflight(on_battery: bool = False, run=_sh, get=httpx.get) -> None`: unless `on_battery`, runs `pmset -g ps` and raises `PreflightFailed` ("the laptop is on battery; plug it in, or pass --on-battery") when `AC Power` is not in its output; a missing `pmset` prints a line and skips the check, as the eval driver does. Then `eval_official.check_machine(True, run)` for the VM clock's coarse check. Then raises if `GET /jobs/overview` lists a job in `RUNNING`, `RESTARTING`, `CREATED` or `INITIALIZING` (message: its name and "cancel it, or `make down` and `make up`"), or if `flink-job/target/tenant-baselines-job.jar` is missing.
  - `collect(on_battery: bool, run=_sh, get=httpx.get) -> dict` with exactly these keys: `data` (`"synthetic"`), `date_utc`, `commit`, `dirty` (paths under `docs/performance/` and `docs/performance.md` do not count), `on_battery`, `host` (`cpu` from `sysctl -n machdep.cpu.brand_string`, `cores` from `hw.ncpu`, `memory_gb` from `hw.memsize`, `os` from `sw_vers -productVersion`, `arch` from `uname -m`), `power` (the first line of `pmset -g ps`), `vm` (`cpus`, `memory_gb`, `server_version` from `docker info --format '{{json .}}'`), `other_containers` (running containers whose `com.docker.compose.project` label is not this project's: a count, never a name), `images` (sorted, from `docker compose config --images`), `flink` (`version` from `GET /config`'s `flink-version`, and `parallelism.default`, `taskmanager.numberOfTaskSlots`, `taskmanager.memory.process.size` from `GET /jobmanager/config`).
  - `vm_clock_offset(run=_sh, now=time.time, tries: int = 5) -> dict`: each try reads the host clock, runs `docker compose exec -T jobmanager date +%s.%N`, reads the host clock again; `offset_s` = VM reading − the midpoint of the two host readings, `bracket_s` = their difference. Returns the try with the smallest bracket: `{"offset_s", "bracket_s"}`.
  - `gaps(monotonic_s: list[float], max_gap_s: float = 60) -> list[float]`: every interval between two neighbours longer than `max_gap_s`. Drivers record `time.monotonic()` at each poll.
  - `validity(restarts: int, late_events: int, sleep_gaps: list[float], extra: Iterable[str] = ()) -> tuple[bool, list[str]]`: the reasons a run is invalid, in plain words; valid when the list is empty.
  - `class NotAppendTime(ValueError)`.
  - `read_stamped(topic: str, start: dict[int, int], end: dict[int, int], consumer=None, bootstrap=kafka_io.BOOTSTRAP) -> list[tuple[int, dict]]`: every record of `[start, end)` per partition as `(broker timestamp ms, parsed JSON)`. Its own loop; `kafka_io._read` is not touched. Raises `kafka_io.IncompleteRead` on a gap or a short read, and `NotAppendTime`, naming the topic and `make reset`, when a record's timestamp type is not `confluent_kafka.TIMESTAMP_LOG_APPEND_TIME`.
  - `write_result(name: str, result: dict, run_dir: Path, pilot: bool) -> Path`: writes `<run_dir>/result.json`, and `docs/performance/<name>.json` too unless `pilot`. `json.dumps(..., indent=2, sort_keys=True, allow_nan=False)`.
- `bench_flink` (every function that calls REST takes `get`/`patch` callables defaulting to `httpx`, like `eval_run.wait_finished`):
  - `submit(run_id: str, topic: str, extra: Iterable[str] = ()) -> str`
  - `vertex(job: dict, prefix: str) -> dict`: the one entry of a `GET /jobs/<id>` body's `vertices` whose `name` starts with `prefix`; `KeyError` naming the vertices if none or several. *A vertex chained with its sinks is named `scorer -> (…)`.*
  - `sample(job_id, source_vertex_id, parallelism) -> dict`: one burst. Sends the metrics request that starts the refresh and notes `time.time()` and `time.monotonic()`; sleeps 1 s; reads `"<i>.numRecordsOut"` for `i` in `range(parallelism)`; reads `GET /jobs/<id>` once. Returns `{"t", "mono", "count", "vertices": {name: {"busy_ms", "backpressured_ms", "idle_ms", "parallelism"}}}`. Raises if a counter id has no value.
  - `watermark_lag_s(job_id, offset_s: float) -> float | None`: a fresh read of every `*.minute-aggregator.currentInputWatermark`; `None` when the lowest is negative (no watermark yet); else `request time + offset_s − lowest / 1000`.
  - `late_events(job_id) -> int`; `restarts(job_id) -> int` (`len(exceptionHistory.entries)`).
  - `job_times(job_id) -> dict`: `{"start_ms", "end_ms", "source_end_ms"}` from `start-time` and `end-time` of `GET /jobs/<id>` and of its vertex whose name starts with `Source:`.
  - `checkpoint_config(job_id) -> dict`: `{"interval_ms", "state_backend"}` from `GET /jobs/<id>/checkpoints/config`.
  - `checkpoint_history(job_id) -> list[dict]`: the `COMPLETED` entries of `history`, each `{"id", "trigger_timestamp", "state_size", "checkpointed_size"}`.
  - `latest_before(history: list[dict], before_ms: int | None) -> dict`: the entry with the highest `id` whose `trigger_timestamp < before_ms` (any, when `None`); `ValueError` if there is none.
  - `vertex_state(job_id, checkpoint_id) -> list[dict]`: `{"name", "state_size", "checkpointed_size"}` per vertex, names from `GET /jobs/<id>`.
  - `cancel_and_clean(job_id, run_id) -> None`: `PATCH /jobs/<id>?mode=cancel`, wait up to 60 s for `CANCELED`, then `docker compose exec -T jobmanager rm -rf /flink-data/checkpoints/bench/<run_id>`. A finished or already cancelled job is only cleaned.
  - `taskmanager_memory(run=_sh) -> dict`: `{"used_bytes", "limit_bytes"}`: the container id from `docker compose ps -q taskmanager`, then `docker stats --no-stream --format '{{.MemUsage}}' <id>`.

- [ ] **Step 1: Write the failing tests** (fake `run`, `get` and consumer objects, as `test_eval_official.py` and `test_eval_run.py` do)
  - `test_preflight_refuses_battery`: the fake `pmset -g ps` prints `Now drawing from 'Battery Power'` → `PreflightFailed`; with `on_battery=True` the same fake passes.
  - `test_preflight_refuses_a_running_job`: overview with `{"name": "tenant-baselines-live", "state": "RUNNING"}` → `PreflightFailed` naming the job; with only `FINISHED` and `CANCELED` jobs it passes.
  - `test_collect_counts_other_containers_without_names`: two foreign containers → `other_containers == 2`, and no container name appears anywhere in `json.dumps(result)`.
  - `test_collect_dirty_ignores_the_result_files`: a status listing only `docs/performance/throughput.json` → `dirty` false; with `py/x.py` added → true.
  - `test_clock_offset_takes_the_tightest_bracket`: three tries with host readings `(100.0, 100.4)`, `(101.0, 101.1)`, `(102.0, 102.6)` and VM readings `100.9`, `101.75`, `103.0` → `offset_s == pytest.approx(0.70)`, `bracket_s == pytest.approx(0.1)`.
  - `test_gaps_finds_a_host_sleep`: `[0, 12, 24, 300, 312]` → `[276]`.
  - `test_validity_lists_every_reason`: restarts 1, late 3, one gap, one extra → four reasons, `False`; all clean → `(True, [])`.
  - `test_read_stamped_returns_append_times`; `test_read_stamped_rejects_create_time` (message contains the topic and `make reset`); `test_read_stamped_rejects_a_gap` (`IncompleteRead`).
  - `test_sample_sums_task_level_ids_only`: the metrics payload holds `0.numRecordsOut` 10, `1.numRecordsOut` 20 and `0.Source__events.numRecordsOut` 10 → `count == 30`.
  - `test_vertex_matches_a_chained_name_by_prefix`: `"scorer -> (scores-sink: Writer)"` is found by `"scorer"`; two matches raise `KeyError`.
  - `test_watermark_lag_none_before_first_watermark`: lowest value `-9223372036854775808` → `None`; `test_watermark_lag_adds_the_offset`: request time 1,000.0, offset −0.7, lowest watermark 978,000 ms → `21.3`.
  - `test_latest_before`: triggers `{1: 10, 2: 20, 3: 30}` with `before_ms=25` → id 2; `before_ms=5` → `ValueError`; `test_checkpoint_history_drops_failed_and_in_progress`.
  - `test_vertex_state_maps_ids_to_names`.
- [ ] **Step 2: Run them.** `make test-py` → fail (no modules).
- [ ] **Step 3: Implement.** `bash -n scripts/submit-bench.sh` → no output.
- [ ] **Step 4: Run the tests.** `make test-py && make lint-py` → clean.
- [ ] **Step 5: Commit.** `git add py/src/tenant_baselines/bench_env.py py/src/tenant_baselines/bench_flink.py py/tests/test_bench_env.py py/tests/test_bench_flink.py scripts/submit-bench.sh && git commit -m "Add the benchmark's preflight, environment record and Flink reads"`
- [ ] **Step 6 (needs the stack; run during Task 3's pilot): read what the docs did not confirm.** With a bench job running, one `curl … | python3 -m json.tool` each for: the source vertex's `/metrics` list (expect `0.numRecordsOut` and `1.numRecordsOut`); `/jobs/<id>` (expect `start-time`, `end-time`, and both on each vertex); `/jobs/<id>/checkpoints` (expect `history[].status`, `trigger_timestamp`, `checkpointed_size`); `/jobs/<id>/checkpoints/details/<n>` (expect per-vertex `state_size` and `checkpointed_size` under `tasks`); `/jobs/<id>/checkpoints/config` (expect `interval` and `state_backend`); `/config` (expect `flink-version`). Then `docker compose exec -T jobmanager date +%s.%N` prints seconds with nine decimals, and
  `cd py && uv run python -c "from confluent_kafka import Consumer, TopicPartition, TIMESTAMP_LOG_APPEND_TIME as T; c = Consumer({'bootstrap.servers': 'localhost:9092', 'broker.address.family': 'v4', 'group.id': 'tb-probe', 'enable.auto.commit': False}); c.assign([TopicPartition('eval.alerts', 0, 0)]); m = c.poll(10); print(m.timestamp(), T)"`
  prints a pair whose first element equals `T`. Where a name differs, fix the function and its test; one commit: `git commit -m "Match Flink's REST field names as the running stack has them"`, naming the files.

### Task 3: Throughput

**Implementer:** standard. **Reviewer:** stronger.

**Files:**
- Create: `py/src/tenant_baselines/bench_world.py`
- Modify: `Makefile`
- Create: `py/src/tenant_baselines/bench_fill.py`, `py/src/tenant_baselines/bench_throughput.py`, `py/tests/test_bench_fill.py`, `py/tests/test_bench_throughput.py`
- Test: `py/tests/test_bench_world.py`

**Interfaces:**
- Consumes: Task 2; `kafka_io.create_topic`, `produce_all`, `end_offsets`, `read_range`.
- Produces:
  - `bench_world.build("bench", seed) -> World`: the three demo regions; tenants `b0001`…`b1000`, all `steady=True`, tenant *i* (from 0) homed in region `i % 3`, `peak_rpm=240.0`; 8 shared units per region, `<region>-s1`…`-s8`, base latencies uniform in 30–80 ms from `default_rng([seed, 5])`, rounded to 0.1; tenant *i* has two keys, on its home region's units `s[(i // 3) % 8 + 1]` and `s[((i // 3) + 1) % 8 + 1]`. 2,000 keys, in tenant order.
  - `bench_fill.LATENCY_FACTOR = 1.6`.
  - `bench_fill.fill_events(world, start_ms: int, minutes: int, rpm: float, seed: int, n_partitions: int) -> Iterator[tuple[int, bytes, bytes]]`: `(partition, key as UTF-8 bytes, payload)`, one event minute at a time. Per minute and key: a Poisson count with mean `rpm`; timestamps uniform inside the minute; latency log-normal around the unit's base latency × `LATENCY_FACTOR` with `traffic.LATENCY_SIGMA`; `ok` with probability 0.995; payload from `events.encode`. Partition = the key's index in `world.keys` modulo `n_partitions`. Within each partition the timestamps never decrease. Draws come from `default_rng([seed, 6])`. `start_ms` must be a multiple of 60,000, or `ValueError`.
  - `class WindowTooShort(Exception)`.
  - `window(samples: list[dict], total: int, warmup_s: float = 60, end_share: float = 0.9) -> tuple[int, int]`: the indexes of the window's first and last sample: the first sample at least `warmup_s` after the first one with a count above 0, and the last sample with a count below `end_share × total`. A count lower than the one before raises `ValueError` containing `restarted`.
  - `sustained_rate(samples, total, warmup_s=60, end_share=0.9, min_window_s=120, min_intervals=8) -> dict` with keys `events_per_s`, `window_start_s`, `window_end_s`, `window_s`, `intervals`, `min_interval_rate`, `max_interval_rate`, `timing_error_pct` (`100 × 2 × 1 ÷ window_s`, rounded to 0.1). Times are the samples' `t`. A window shorter than `min_window_s` or with fewer than `min_intervals` intervals raises `WindowTooShort` whose message contains `BENCH_MINUTES`.
  - `window_shares(samples, first: int, last: int) -> dict`: per vertex name, `{"busy", "backpressured", "idle"}`: the rise of each accumulated value between the two samples ÷ (`parallelism` × the window in ms).
  - `check_final_count(count: int, total: int) -> None`: `ValueError` naming both numbers and "not the task-level record count" unless they are equal.
  - `alert_counts(rows: list[dict]) -> dict`: `firing` rows counted by `"<detector>/<setting>/<scope>"`.
  - `main()`: `--minutes` (default 150, at most 300), `--rpm` (120), `--runs` (2), `--seed` (1), `--on-battery`, `--pilot`.
  - `docs/performance/throughput.json`: `{"data", "env", "valid", "invalid_reasons", "fill": {"topic", "keys", "partitions", "event_minutes", "rpm_per_key", "events", "seconds", "events_per_s", "kafka_bytes", "bytes_per_event"}, "warmup_s", "end_share", "sample_interval_s", "settings", "runs": [{"run_id", "job_id", "events", …sustained_rate(...), "source": {"backpressured", "idle", "busy"}, "drop_ticks_busy", "shares": window_shares(...), "busiest_vertex", "checkpoint": {"id", "state_size", "checkpointed_size", "vertices"}, "taskmanager_memory", "alert_rows": alert_counts(...), "late_events", "restarts", "host_sleep_gaps_s"}], "spread_pct"}`. `spread_pct` = `(max − min) ÷ mean × 100` of the runs' rates.

**Behaviour of the driver:**
1. `bench_env.preflight`, `bench_env.collect`.
2. `fill_id = "tp-<UTC yyyymmddThhmmss>"`. Read the free space and the size of the broker's data directory (`docker compose exec -T kafka df -Pk /var/lib/kafka/data`, the fourth field of the second line, and `du -sk /var/lib/kafka/data`; both in KiB, times 1,024; POSIX options, since the broker image may lack GNU `df`/`du`; Task 2 Step 6 runs both once on the stack). Refuse when the free space is under 2 × `minutes × 240,000 × BYTES_PER_EVENT`, where `BYTES_PER_EVENT = 60` is an estimate the pilot replaces. Create `<fill_id>.events` with 8 partitions. `start_ms` = the current minute's start − `(minutes + 1) × 60,000`. Produce `fill_events(...)` with `produce_all`, timing it. Check that the end offsets add up to the count produced; fail otherwise. Read the directory's size again: `kafka_bytes` is the growth.
3. For each run `n`: run id `<fill_id>-r<n>`; note `end_offsets("bench.alerts")`; `submit`; wait for `RUNNING`. Then every 12 s on the monotonic clock take one `bench_flink.sample` and append it, with no REST call between samples, until the count reaches the events filled or 30 minutes pass (then cancel, clean and fail). At the end: `check_final_count`; wait 11 s (Flink's metric cache), then `late_events`, `restarts`; `latest_before(checkpoint_history, None)` with `vertex_state`; `taskmanager_memory`; `read_range("bench.alerts", …)` filtered to this `run_id` for `alert_counts`. Then `cancel_and_clean`. The cancel comes within a sample or two of the end, before the partitions' 60 s idleness lets the ticks jump the watermark to the wall clock.
4. Per run: `window`, `sustained_rate`, `window_shares`; `source` is the shares of the vertex starting `Source: events`; `drop_ticks_busy` is the busy share of the vertex starting `drop-ticks`; `busiest_vertex` is the name with the highest busy share. `gaps` over the samples' `mono`.
5. `settings` = 1 + the distinct `(detector, setting)` pairs in `infra/flink/thresholds-demo.json`.
6. Write the samples to `data/bench/<fill_id>/samples-r<n>.json`, and the result through `write_result("throughput", …)`. Print one line per run.

- [ ] **Step 1: Write the failing tests**
  - `test_bench_world.py::test_bench_world_has_two_thousand_keys`: 2,000 distinct `key` strings; 1,000 tenants; 24 units; every tenant's two keys are on two different units of its home region; the regions have 668, 666 and 666 keys; the same seed gives the same world.
  - `test_bench_world.py::test_bench_latencies_stay_under_the_static_threshold`: for seeds 1–5, `max(u.base_latency_ms) * LATENCY_FACTOR * exp(1.645 * LATENCY_SIGMA)` is below the `latency_p95` fire level of every set in `infra/flink/thresholds-demo.json`, read from the file.
  - `test_bench_world.py::test_measured_paths_equal_the_freeze` *(a pin)*: `git diff --quiet <eval/freeze.json's commit> HEAD -- <freeze.MEASURED_PATHS>` exits 0; skipped when `git` or that commit is not available (the CI container copies the tree without `.git`).
  - `test_bench_world.py::test_eval_demo_and_paths_worlds_are_unchanged` *(a pin: it passes before the change)*: `build_world("eval", 7)`, `("demo", 1)` and `("paths", 7)` have 48, 22 and 10 keys.
  - `test_bench_fill.py::test_time_order_within_each_partition`: `fill_events(bench_world.build("bench", 1), 1_800_000_000_000 // 60_000 * 60_000, 3, 120, 1, 8)`; decode each payload's `ts`; per partition the list is sorted; every `ts` is inside `[start, start + 180_000)`; every key is `bytes`.
  - `test_count_is_near_the_rate`: the same call yields a count within 1% of 720,000.
  - `test_same_seed_same_events`, and a different seed gives different events. `test_start_must_be_on_a_minute`: `start_ms=1` raises `ValueError`.
  - `test_sustained_rate_excludes_warmup_and_the_last_tenth`: samples with `t` and `count` `(0, 0), (12, 0), (24, 600_000)`, then +1,200,000 every 12 s up to `(264, 24_600_000)`, with `total=25_000_000`. 90% is 22,500,000, so the window ends at `(240, 22_200_000)`. Expect `events_per_s == 100_000.0`, `window_start_s == 84`, `window_end_s == 240`, `intervals == 13`, `min_interval_rate == max_interval_rate == 100_000.0`, `timing_error_pct == 1.3`.
  - `test_window_too_short_raises`: the same shape with `total=10_000_000` → `WindowTooShort`, message contains `BENCH_MINUTES`.
  - `test_counter_going_backwards_raises`: counts `2_000_000` then `100` → `ValueError` containing `restarted`.
  - `test_final_count_must_equal_the_fill`: `check_final_count(999, 1000)` and `(1001, 1000)` raise; `(1000, 1000)` returns.
  - `test_window_shares`: a vertex with parallelism 2 whose `busy_ms` rises from 10,000 to 130,000 over a 120 s window → `busy == 0.5`.
  - `test_alert_counts`: two `firing` rows of `global_static/p99.9` tenant scope, one `resolved`, one seasonal regional `firing` → `{"global_static/p99.9/tenant": 2, "seasonal/k4/region": 1}`.
- [ ] **Step 2: Run them.** `make test-py` → the new tests fail (`unknown world: 'bench'`, no modules); the pin passes.
- [ ] **Step 3: Implement.** Vectorise the fill per minute with numpy and sort once per minute and partition, as `LiveGenerator.minute` does. Add to the `Makefile`, and add `bench-throughput` to `.PHONY`:
  ```make
  BENCH_MINUTES ?= 150
  # The throughput benchmark (synthetic data): pre-fills a topic, then reads it twice with the
  # live job. Needs AC power, no other job on the cluster, and the Flink web UI closed.
  bench-throughput: jar up
  	cd py && $(AWAKE) uv run python -m tenant_baselines.bench_throughput \
  		--minutes $(BENCH_MINUTES) $(BENCH_ARGS)
  ```
- [ ] **Step 4: Run the tests.** `make test-py && make lint-py` → clean.
- [ ] **Step 5: Commit.** `git add py/src/tenant_baselines/bench_world.py py/src/tenant_baselines/bench_fill.py py/src/tenant_baselines/bench_throughput.py py/tests/test_bench_world.py py/tests/test_bench_fill.py py/tests/test_bench_throughput.py Makefile && git commit -m "Add the throughput benchmark and its 2,000-key world"`
- [ ] **Step 6: Pilot, on the stack.** Run `pmset -g ps` and read it: AC power. `colima status`. `make jar up`. If a demo job is running, `make down && make up`. Close the Flink web UI. Then:
  `cd py && uv run python -m tenant_baselines.bench_throughput --minutes 30 --runs 1 --pilot`
  Expected: one rate, or `WindowTooShort` (30 minutes is about 7 million events). Do Task 2 Step 6 while the job runs. Then read `data/bench/<fill_id>/`:
  - The interval rates in the samples do not alternate high and low, and the last intervals of the window are not lower than the rest. If they alternate, something else is reading metrics; find it before going on.
  - The source's idle share is small. If the source is mostly idle, the broker or the fetch is the limit; say so in the write-up's method notes and do not call the rate the job's.
  - `alert_rows` is empty or nearly so. A static latency or volume alert on many keys means the world is crossing a demo threshold; fix the world, not the thresholds.
  - Set `BYTES_PER_EVENT` to the measured `bytes_per_event`, rounded up to the next 10.
  - Set `BENCH_MINUTES` so one run lasts at least 5 minutes: `ceil(rate × 300 ÷ 240,000 ÷ 10) × 10`, not below 150 and not above 300. At the cap, a run shorter than 5 minutes is accepted as long as the window rule holds.
  Commit the two constants if they changed: `git add Makefile py/src/tenant_baselines/bench_throughput.py && git commit -m "Size the throughput pre-fill from the pilot"`. If the job restarted or `lateEvents` was not 0, stop and find out why.

### Task 4: Delay: the world, the schedule, the labels and the measures

**Implementer:** standard. **Reviewer:** stronger.

**Files:**
- Modify: `py/src/tenant_baselines/bench_world.py` (from Task 3), `py/src/tenant_baselines/gen_live.py`
- Create: `py/src/tenant_baselines/bench_delay.py`, `py/tests/test_bench_delay.py`
- Test: `py/tests/test_bench_world.py`, `py/tests/test_gen_live.py`

**Interfaces:**
- Consumes: `incidents.Incident`; `scoring.nearest_rank`.
- Produces:
  - `bench_world.build("delay", seed) -> World`: the three demo regions; tenants `d01`…`d36`, all `steady=True`, tenant *i* homed in region `i % 3`, `peak_rpm=40.0`; two shared units per region with base latencies 70.0 (`-s1`) and 90.0 (`-s2`); one key per tenant, on `-s1` when `(i // 3) % 2 == 0`, else `-s2`. The seed is not used.
  - In `bench_delay`:
    - `SETTLE_MS = 300_000`, `SPACING_MS = 180_000`, `REGION_OFFSET_MS = 60_000`, `DURATION_MS = 360_000`, `MAGNITUDE = 0.20`, `MATCH_GRACE_MS = 600_000`, `TAIL_MS = 180_000`, `SIGNAL = "error_rate"`, `MIN_MATCHED = 0.9`.
    - `DESIGN = {"watermark_bound_s": 20, "idleness_s": 60, "fire_after_minutes": 3, "hold_s": 120, "group_wait_s": 30}`, each with a comment naming its source: `Watermarks.BOUND`, `Watermarks.LIVE_IDLENESS`, `AlertStateMachine.FIRE_AFTER`, `RegionGrouper.HOLD_MS`, `infra/alertmanager/alertmanager.yml`.
    - `schedule(world, t0_ms: int, n: int, seed: int) -> list[Incident]`: incident *j* is in region `j % 3` and is that region's incident `i = j // 3`; its key is the region's `i % len(keys)`-th key in world order; `start_ms = t0_ms + SETTLE_MS + i × SPACING_MS + (j % 3) × REGION_OFFSET_MS + jitter_j × 1000`, with `jitter_j` an integer in 0–59 from `default_rng([seed, 7])`, drawn in order of *j*; `end_ms = start_ms + DURATION_MS`; `Incident("error_spike", start, end, MAGNITUDE, key=...)`.
    - `labels(incidents, world, t0_ms) -> dict`: `{"data": "synthetic", "t0_ms", "incidents": [{"id": j, "key", "region", "signal": SIGNAL, "start_ms", "end_ms"}]}`.
    - `run_end_ms(labels) -> int`: the last `end_ms + TAIL_MS`.
    - `first_appends(records: list[tuple[int, dict]]) -> list[tuple[int, dict]]`: one entry per `(alert_id, state)`, the one with the earliest append time.
    - `measure(labels, records, detector: str, setting: str) -> dict`: keys `incidents`, `matched`, `missed` (ids), `unmatched_firing_rows` (tenant `firing` rows of that detector and setting with `event_time_ms ≥ t0_ms + SETTLE_MS` that matched nothing), `regional_firing_rows` (the same window, scope `region`), `pipeline_delay_s`, `time_to_page_s`, `emit_delay_s` (`emitted_at_ms − event_time_ms`), `sink_delay_s` (append − `emitted_at_ms`), each of the four a `summary`; and `under_five_minutes` (matched incidents with time-to-page below 300 s).
    - `summary(values_s: list[float]) -> dict`: `{"n", "p50", "p95", "min", "max"}`, nearest-rank, rounded to 0.1; `n == 0` gives `None` for the rest.
    - `pairs(records) -> list[tuple[str, str]]`: the sorted `(detector, setting)` pairs among the `firing` rows.
    - `enough_matched(m: dict) -> bool`: `m["matched"] >= MIN_MATCHED * m["incidents"]`.
  - In `gen_live`:
    - `take_step(next_ms: int, now_ms: int) -> tuple[str, int, int]`: `(step, start_ms, next_ms after it)` with `step` from `next_step`; for `"wait"` the last two are both `next_ms`. `main`'s loop uses it. *This changes the loop every mode runs. The delay pilot covers it with history and live traffic; `make demo` is not run in this milestone, and one `make smoke` on this loop goes on the milestone 6 list (Task 7).*
    - `--world` accepts `demo` and `delay`. `--script` accepts `demo`, `none` and `bench`. `--bench-incidents N` (default 60). `--labels-out PATH`, required with `--script bench` (argparse error otherwise).
    - With `--script bench`: at catch-up, `t0 = next_ms`; `schedule(world, t0, n, seed)` becomes the incident list; the labels are written to a temporary file beside `PATH` and renamed onto it; the log line is `caught up after … s, … events; bench script: N incidents from HH:MM to HH:MM UTC`. The producer gets `"delivery.timeout.ms": 60_000`, and the 10 s log block exits with status 1 and an error line when `stats.failed > 0`.
    - With `--script demo` or `none`, the producer's settings, the catch-up branch and the log lines are what they are today.

- [ ] **Step 1: Write the failing tests**
  - `test_bench_world.py::test_delay_world_has_twelve_tenants_a_region`: 36 keys, 12 per region, every tenant steady with `peak_rpm == 40.0` and exactly one key; `{LiveGenerator(world, 1, []).partition_of(k.key, 8) for k in world.keys} == set(range(8))`.
  - `test_schedule_shape`: `schedule(bench_world.build("delay", 1), 1_800_000_000_000, 60, 1)` → 60 incidents; 20 per region; every one `error_spike`, magnitude 0.20, 360,000 ms long; the earliest start is at or after `t0 + 300_000`; inside a region, consecutive starts are more than 120,000 and less than 240,000 ms apart; the same key's starts are at least 2,100,000 ms apart; `len({s.start_ms % 60_000 for s in incidents}) >= 20`; a second call returns the same list.
  - `test_schedule_never_reaches_half_a_region`: for every region and every incident start, the incidents of that region whose `[start + 180_000, start + 720_000)` contains that instant number at most 4.
  - `test_labels_round_trip`: `json.loads(json.dumps(labels(...)))` has 60 incidents with ids 0–59 and `signal == "error_rate"`.
  - `test_design_constants_match_the_sources`: read `Watermarks.java`, `AlertStateMachine.java`, `RegionGrouper.java` and `alertmanager.yml` as text and assert `Duration.ofSeconds(20)` follows `BOUND`, `Duration.ofSeconds(60)` follows `LIVE_IDLENESS`, `FIRE_AFTER = 3`, `HOLD_MS = 120_000L`, `group_wait: 30s`.
  - `test_first_appends_keeps_the_earliest`: the same `(alert_id, "firing")` at append 1,000 and 5,000 → one entry, 1,000.
  - `test_measure_two_definitions`: one incident, start `S = 1_800_000_320_000` (second 20 of its minute), on `d01|us-east|us-east-s1`; a `seasonal`/`k4` tenant `firing` row on that key and `error_rate` with `event_time_ms = S − 20_000 + 300_000`, `emitted_at_ms = event_time + 21_000`, appended at `event_time + 21_500` → `pipeline_delay_s["p50"] == 21.5`, `time_to_page_s["p50"] == 301.5`, `emit_delay_s["p50"] == 21.0`, `sink_delay_s["p50"] == 0.5`, each with `n == 1`; `under_five_minutes == 0`. Moving the start to second 40 gives `time_to_page_s["p50"] == 281.5` and `under_five_minutes == 1`.
  - `test_measure_counts_misses_unmatched_and_regional`: two incidents and four rows: one matching the first; one on a third key; one of scope `region`; one on the second incident's key with `event_time` before its start → `matched == 1`, `missed == [1]`, `unmatched_firing_rows == 2`, `regional_firing_rows == 1`.
  - `test_measure_ignores_other_detectors_and_signals`: a `global_static` row and a `latency_p95` row on the right key do not match for `seasonal`/`k4`.
  - `test_match_window_ends_ten_minutes_after_the_incident`: `event_time == end + 600_000` matches; one millisecond later does not.
  - `test_run_is_invalid_under_ninety_percent`: 53 of 60 matched → `enough_matched` false; 54 → true.
  - `test_summary_nearest_rank`: values 1…20 → `p50 == 10`, `p95 == 19`, `n == 20`; `[]` → `n == 0`, `p50 is None`.
  - `test_gen_live.py::test_steps_cross_the_seam_without_gap_or_overlap`: start `next_ms` on a minute boundary 150 s behind a fake clock that advances 7 s per `"minute"` step and 0 otherwise; loop `take_step` until `"wait"`; the `(start, next)` intervals are contiguous from the first start to the last `next`; every `"minute"` step starts on a minute boundary; the last `next` is within 1,000 ms of the clock.
  - `test_gen_live.py::test_bench_script_needs_labels_out`: `--world delay --script bench` without `--labels-out` exits with code 2 and its stderr names `--labels-out` (the exit code alone cannot go RED: `--world delay` already exits 2 today). The existing `test_live_generator_rejects_non_demo_worlds` (worlds `eval` and `paths`) stays as it is.
  - `test_gen_live.py::test_delay_world_tick_rate`: over 2,000 ticks, a `delay` key's count per tick is within 10% of 40 ÷ 60.
- [ ] **Step 2: Run them.** `make test-py` → fail.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests.** `make test-py && make lint-py` → clean, the existing `test_gen_live.py` tests included.
- [ ] **Step 5: Commit.** `git add py/src/tenant_baselines/bench_world.py py/src/tenant_baselines/gen_live.py py/src/tenant_baselines/bench_delay.py py/tests/test_bench_world.py py/tests/test_gen_live.py py/tests/test_bench_delay.py && git commit -m "Add the delay benchmark's world, schedule, labels and measures"`

### Task 5: The delay driver, the watermark-lag message, and the pilot

**Implementer:** standard. **Reviewer:** stronger throughout.

**Files:**
- Create: `py/src/tenant_baselines/bench_delay_run.py`, `py/tests/test_bench_delay_run.py`
- Modify: `scripts/watermark-lag.sh`, `Makefile`

**Interfaces:**
- Consumes: Tasks 2 and 4.
- Produces:
  - `plan(labels: dict) -> dict`: `{"first_start_ms", "read_at_ms"}` with `read_at_ms = run_end_ms(labels)`.
  - `lag_summary(samples_s: list[float]) -> dict`: `{"samples", "min", "median", "max"}`, rounded to 0.1.
  - `offset_moved(before: dict, after: dict, limit_s: float = 1.0) -> bool`.
  - `generator_command(run_id: str, incidents: int, out_dir: Path) -> list[str]`: `docker compose --profile demo run -d --no-deps --name tb-<run_id> -v <out_dir>:/out generator python -m tenant_baselines.gen_live --bootstrap kafka:19092 --topic <run_id>.events --world delay --seed 1 --script bench --bench-incidents <n> --labels-out /out/labels.json --history-days 21 --partitions 8`.
  - `main()`: `--incidents` (default 60), `--on-battery`, `--pilot`.
  - `docs/performance/delay.json`: `{"data", "env", "valid", "invalid_reasons", "run_id", "job_id", "world": {"keys", "rpm_per_key", "partitions", "history_days"}, "incidents", "schedule": {"settle_s", "spacing_s", "duration_s", "magnitude", "tail_s"}, "design": bench_delay.DESIGN, "settings" (computed as in the throughput driver: 1 + the distinct `(detector, setting)` pairs in `infra/flink/thresholds-demo.json`), "headline": {"detector": "seasonal", "setting": "k4", …measure(...)}, "by_detector": {"<detector>/<setting>": measure(...)}, "watermark_lag_s": lag_summary, "clock_offset": {"before", "after"}, "generator": {"caught_up_ms", "failed_deliveries"}, "late_events", "restarts", "host_sleep_gaps_s"}`.

**Behaviour of the driver.** One rule covers every failure after step 2: cancel the job and clean its checkpoints if it was submitted, stop and remove the generator container, then fail.
1. `preflight`, `collect`, `vm_clock_offset` (before). Run id `delay-<UTC yyyymmddThhmmss>`. Create `<run_id>.events` with 8 partitions (`kafka_io.create_topic`) and `data/bench/<run_id>/`.
2. Note `end_offsets("bench.alerts")`. Start the generator container (`generator_command`).
3. Wait up to 120 s until every partition of the events topic has data, as `scripts/demo.sh` does and for its reason; then `bench_flink.submit(run_id, topic)`.
4. Every wait in steps 3 and 4 polls every 10 s and records `time.monotonic()` at each poll, so `gaps` (60 s) sees no false sleep. Wait up to 15 minutes for `labels.json`. Then wait until `watermark_lag_s` is below 60. If that has not happened one minute before the first incident's start, fail: the job did not catch up in time.
5. Until `read_at_ms` on the VM's clock (host time + the offset): every 30 s on the monotonic clock, record `time.monotonic()`; on every second poll take one `watermark_lag_s(job_id, offset)` sample (kept only from the first incident's start on); and check that the generator container is still running. If it exited, fail. No other REST call in this loop.
6. At `read_at_ms`: `read_stamped("bench.alerts", start, end_offsets(...))`, keep the rows of this `run_id`; wait 11 s (Flink's metric cache), then `late_events`, `restarts`; `docker stop` the generator, read its log for `delivery failed`, `docker rm` it; `cancel_and_clean`; `vm_clock_offset` (after).
7. `first_appends`, then `measure` for `seasonal`/`k4` (the headline) and for every pair of `pairs(records)`. `validity(...)` with these extra reasons where they apply: "fewer than 90% of the incidents were matched" (headline), "the VM clock moved against the host's by more than 1 s", "the generator reported failed deliveries". `write_result("delay", …)`. Print the headline's two summaries.
- `scripts/watermark-lag.sh`: a sample whose lowest watermark is negative still exits 1 with `no watermark yet`. A sample more than 86,400 s behind with a real watermark exits 1 with `the watermark is N.N days behind the wall clock: the job is replaying history`. The header comment says both, and that the lag is against the host's clock, so a VM clock that is off shifts it by the same amount.

- [ ] **Step 1: Write the failing tests:** `test_plan_reads_three_minutes_after_the_last_incident`; `test_lag_summary` (`[21.1, 22.5, 25.7]` → min 21.1, median 22.5, max 25.7, samples 3); `test_offset_moved` (−0.70 to −0.20 → false; −0.70 to 0.50 → true); `test_generator_command_runs_in_the_compose_image` (contains `generator`, `--world`, `delay`, `--labels-out`, `/out/labels.json`, and a `-v` whose host side is the given directory).
- [ ] **Step 2: Run them.** `make test-py` → fail.
- [ ] **Step 3: Implement,** the script change included. Add to the `Makefile`, and add `bench-delay` to `.PHONY`:
  ```make
  BENCH_INCIDENTS ?= 60
  # The delay benchmark (synthetic data): three weeks of history, then live traffic at a fixed
  # rate with injected incidents. About 80 minutes. Needs AC power and the lid open throughout.
  bench-delay: jar up
  	docker compose --profile demo build generator
  	cd py && $(AWAKE) uv run python -m tenant_baselines.bench_delay_run \
  		--incidents $(BENCH_INCIDENTS) $(BENCH_ARGS)
  ```
- [ ] **Step 4: Run the tests.** `make test-py && make lint-py` → clean; `bash -n scripts/watermark-lag.sh` → no output.
- [ ] **Step 5: Commit.** `git add py/src/tenant_baselines/bench_delay_run.py py/tests/test_bench_delay_run.py scripts/watermark-lag.sh Makefile && git commit -m "Add the delay benchmark's driver; tell a history replay from no watermark"`
- [ ] **Step 6: Pilot, on the stack (about 21 minutes).** `pmset -g ps`: AC power; lid open. `docker compose --profile demo build generator`, then:
  `cd py && uv run python -m tenant_baselines.bench_delay_run --incidents 6 --pilot`
  While the history replays, the driver is only waiting on files and offsets, so run `scripts/watermark-lag.sh 1 11` once: it must print the new "replaying history" message. Do not run it after the labels file appears. Expected at the end: `valid: true`; 6 of 6 matched for `seasonal`/`k4`; `regional_firing_rows` 0; pipeline delay p50 between 20 and 30 s; time-to-page p50 between 270 and 340 s, with some incidents under 300 s; the watermark lag's median between 20 and 25 s; the clock offset's bracket under 0.5 s. If an incident is missed, read its key's rows in `bench.alerts` and find the cause before the sitting; if the cause is the schedule (for example a region reaching 50%), fix the constants in `bench_delay.py` with their tests and note the change as a builder's call in the write-up's method notes. If time-to-page falls outside the range, find out which of the code's steps the expectation got wrong and correct this plan's expectation; do not change a detector setting.

### Task 6: State size

**Implementer:** standard. **Reviewer:** stronger.

**Files:**
- Create: `py/src/tenant_baselines/bench_state.py`, `py/tests/test_bench_state.py`
- Modify: `Makefile`

**Interfaces:**
- Consumes: `eval_run.prepare`, `eval_run.submit` (its `extra` argument), `eval_run.wait_finished`, `freeze.container_path`; `simulate.rows`, `checked`, `encode_row`; `kafka_io.create_topic`, `produce_all`, `end_offsets`; `freeze.load_freeze`; Task 2's `job_times`, `checkpoint_config`, `checkpoint_history`, `latest_before`, `vertex_state`, `vertex`, `preflight`, `collect`, `validity`, `write_result`.
- Produces:
  - `SPEC_ESTIMATE_BYTES_PER_KEY = 3 * 168 * 5 * 60 * 4` (604,800; §4.3's estimate).
  - `EXTRAPOLATE_KEYS = (2_000, 20_000, 100_000)`, `LAST_SHARE = 0.15`.
  - `in_last_share(trigger_ms: int, start_ms: int, source_end_ms: int, share: float = LAST_SHARE) -> bool`.
  - `per_key(vertices: list[dict], keys: int) -> dict`: `{"scorer_bytes", "per_key_bytes", "other_vertices": [{"name", "state_size"}]}`; the scorer is the vertex whose name starts with `scorer`.
  - `size_range(history: list[dict], start_ms: int, source_end_ms: int) -> dict`: `{"checkpoints", "min", "max"}` of `state_size` over the completed checkpoints in the last share, triggered before the source ended.
  - `count_settings(thresholds: dict, seasonal_k: list) -> int`: `len(seasonal_k)` + the distinct `(detector, setting)` pairs of `thresholds["sets"]`.
  - `extrapolate(per_key_bytes: float, keys: Iterable[int]) -> list[dict]`: `{"keys", "bytes"}` each, `bytes = round(per_key_bytes × keys)`.
  - `docs/performance/state.json`: `{"data", "env", "valid", "invalid_reasons", "world": {"name": "eval", "seed": 7, "amplitude": 6.0, "keys", "keys_with_five_weeks", "weeks": 5, "aggregate_rows"}, "settings", "spec_estimate_bytes_per_key", "runs": [{"backend", "interval_ms", "run_id", "job_id", "job_seconds", "checkpoint": {"id", "trigger_share", "state_size", "checkpointed_size", "vertices"}, "range": size_range(...), "per_key": per_key(...)}], "headline_backend": "hashmap", "extrapolation": {"label": "extrapolation, not measured", "hashmap": [...], "rocksdb": [...]}}`.

**Behaviour of the driver:** `preflight`, `collect`. For `backend` in `hashmap`, `rocksdb`: run id `state-<backend>-<UTC stamp>`; `prepare("dev", "eval", 7, 6.0, data/bench/<run_id>)`; create `<run_id>.aggregates-in` with one partition and produce the rows as `eval_run.run` does; `submit(run_id, "aggregates", topic, thresholds, extra=[...])` with `-Dexecution.checkpointing.interval=5s`, plus `-Dstate.backend.type=hashmap` for the first; `wait_finished`; `job_times`; `checkpoint_config` (`backend` and `interval_ms` in the result are what it returns; fail if the backend is not the one asked for); `checkpoint_history`; `latest_before(history, source_end_ms)`; fail unless `in_last_share`; `vertex_state`; `per_key(vertices, keys)`; `size_range`. `trigger_share` = `(trigger − start) ÷ (source_end − start)`. `keys` and `keys_with_five_weeks` are counted from the world (`start_week == 1`). `settings` = `count_settings` of the run's `thresholds.json` and `load_freeze()["seasonal_k"]`. The alert rows the job writes to `eval.alerts` are not read. `validity` from `restarts` alone (`eval_run.wait_finished` already refuses a restarted job). `write_result("state", …, pilot=False)`: there is no pilot flag here.

- [ ] **Step 1: Write the failing tests**
  - `test_in_last_share`: start 0, source end 100,000: a trigger at 90,000 → true; at 80,000 → false; at 100,500 → false.
  - `test_per_key_divides_the_scorer_only`: vertices `scorer` 48,000,000, `population-baselines` 2,000,000, `region-grouper -> (alerts-sink: Writer)` 500,000, `Source: aggregates` 100, with 48 keys → `per_key_bytes == 1_000_000`, and `other_vertices` lists the other three with their sizes.
  - `test_per_key_needs_a_scorer_vertex`: no vertex starting `scorer` → `KeyError`.
  - `test_size_range`: sizes 5, 9, 7 triggered at 86,000, 91,000, 96,000 and one of 3 at 50,000, with start 0 and source end 100,000 → `{"checkpoints": 3, "min": 5, "max": 9}`.
  - `test_count_settings`: sets `global_static/p99` twice (two validity windows) and `per_key_static/p99`, with `seasonal_k=[3, 4, 5, 6]` → 6.
  - `test_extrapolate`: `extrapolate(1_000_000, (2_000,))` → `[{"keys": 2000, "bytes": 2_000_000_000}]`.
  - `test_spec_estimate`: `SPEC_ESTIMATE_BYTES_PER_KEY == 604_800`.
- [ ] **Step 2: Run them.** `make test-py` → fail.
- [ ] **Step 3: Implement,** and add to the `Makefile`, with `bench-state` in `.PHONY`:
  ```make
  # The state-size measurement (synthetic data): a dev seed's five weeks through the job twice,
  # checkpointing every 5 s, on the hashmap and the RocksDB backend. About 6 minutes.
  bench-state: jar up
  	cd py && $(AWAKE) uv run python -m tenant_baselines.bench_state $(BENCH_ARGS)
  ```
- [ ] **Step 4: Run the tests.** `make test-py && make lint-py` → clean.
- [ ] **Step 5: Run it.** `pmset -g ps`: AC power. `make bench-state`. Expected: two runs, each with a checkpoint in the last 15% before the source ended, and `docs/performance/state.json` written. If a 5 s interval makes a run restart, use 10 s and say so in the write-up's method notes. The sitting in Task 7 writes the file again.
- [ ] **Step 6: Commit.** `git add py/src/tenant_baselines/bench_state.py py/tests/test_bench_state.py Makefile docs/performance/state.json && git commit -m "Add the state-size measurement"`

### Task 7: The report, the measured sitting, the docs and the follow-ups

**Implementer:** standard. **Reviewer:** standard, checking every sentence that carries a number against the JSON key it reads.

**Files:**
- Create: `py/src/tenant_baselines/bench_report.py`, `py/tests/test_bench_report.py`, `docs/performance.md` (rendered)
- Modify: `Makefile`, `README.md`, `docs/superpowers/m1-followups.md`, `m2-followups.md`, `m3-followups.md`

**Interfaces:**
- Consumes: the three result files' schemas (Tasks 3, 5, 6).
- Produces:
  - `render(throughput: dict, delay: dict, state: dict) -> str`: the whole of `docs/performance.md`.
  - `main()`: `--results DIR --out FILE`. Raises on a missing file, on `"valid": false` (naming the file and its reasons) and on `"data" != "synthetic"`.

**`docs/performance.md`, in this order** (every figure read from the files; plain sentences; no adjective about speed):
1. **What this is.** Three measurements on one laptop, on synthetic data. They describe this build on this machine. They are not evidence about any real system or any larger deployment.
2. **Machine and versions.** A table from `env`: CPU, cores, memory, OS; the Docker VM's CPUs and memory; how many other containers were running in the VM; image tags; Flink version, parallelism, task slots, TaskManager memory; commit; date; power source, with "on battery" printed when `on_battery` is true. One column per measurement when any row differs between the three files. The state runs' backend and checkpoint interval come from `state.runs[]`, not from this table.
3. **Throughput.** What was filled (keys, events, event minutes, per-key rate) and how fast the generator filled it. Then: the topic was complete before the job started and nothing produced during it. The runs in a table: sustained events a second, window length, lowest and highest 12 s interval, and the source's back-pressured and idle shares of the window. The spread between the runs, with the note that the first job on a TaskManager is the colder one. The timing error from `timing_error_pct`. The busiest vertex. The `drop-ticks` vertex's busy share, as an upper bound on what the events/ticks union costs in CPU, with the reason it is an upper bound. How many settings the job ran (`settings`). The alert rows each run wrote, by detector. The TaskManager's memory against its limit, the first reading for one job and the second as cumulative. The checkpoint's size per vertex at 2,000 keys, with the note that the baselines are cold after the fill's event time and that no unit/tenant pair goes stale in this world. What shares the VM: the broker, ClickHouse reading the bench topics, and the other containers.
4. **Delay.** The world and the incident schedule, from `world` and `schedule`. The two definitions, each in one sentence. A table: detector and setting, incidents, matched, pipeline delay p50 and p95, time-to-page p50, p95 and minimum, each with `n`. Then, with the file's numbers in them:
   - How time-to-page is built, from `design`, marked configured, not measured: the detector fires after `fire_after_minutes` scored minutes in a row, and the alert is then held for `hold_s` seconds; both are counted from the start of the first breaching minute, not from the incident's start. A spike of this size usually breaches in the minute it starts in, so the count often starts up to a minute before the incident. The measured minimum, and how many incidents were under 5 minutes (`under_five_minutes`). The spec's §6 says "at least about 5 minutes"; that holds from the start of the first breaching minute.
   - The watermark lag measured during the run: min, median and max from `watermark_lag_s`, corrected by the VM clock's offset (`clock_offset`). By design, marked configured: the lag is `watermark_bound_s` plus the age of the newest event on the slowest partition that is not idle. This benchmark sends at a fixed rate, so every partition is busy. A sparse partition adds up to `idleness_s` seconds; that case was not measured here.
   - Both figures end at the append to the alerts topic. The forwarder and Alertmanager's `group_wait` (`group_wait_s`, configured) come after it. The README's milestone 3 page times were measured at the webhook, from the incident's start minute, and are not comparable.
   - The split of the pipeline delay: decision to emit (`emit_delay_s`), emit to append (`sink_delay_s`).
   - Misses, unmatched rows and regional rows, as counts.
5. **State size.** The world (`keys`, `keys_with_five_weeks`; §6 says 60, see the plan). The hashmap run first, as the headline: which checkpoint, at what share of the run it was triggered, total size, size per vertex, the scorer's bytes per key. Then RocksDB: the same, plus the range over the last checkpoints, and one sentence on why its figure moves (SST files not yet compacted are counted). The spec's estimate of bytes per key beside both, labelled an estimate. How many settings this job ran against the live job's (`state.settings`, `throughput.settings`), and that the baseline history, which all settings share, is most of a key's state.
6. **Extrapolation, not measured.** The table from `state.extrapolation`, and: it is the scorer's bytes per key times the key count; it assumes every key has a full 4-week history and that state grows linearly with keys, and neither was tested above the measured key count; it leaves out the region grouper's per-key activity, the minute aggregator's state in events mode and the population baselines.
7. **What these numbers do not show.** One laptop; a single broker on the same VM; parallelism 2; synthetic traffic; throughput with cold baselines; no sparse partition; no failover during the delay run. A host sleep steps the VM's clock, and Flink then restarts the job; a run in which that happened is discarded.
8. **Reproduce.** `make bench`, and the three targets it runs, with the run conditions.

- [ ] **Step 1: Write the failing tests** (fixtures: three small hand-made result dicts in the test file)
  - `test_render_has_every_required_statement`: the output contains `synthetic`, `configured, not measured`, `start of the first breaching minute`, `not measured here`, `Extrapolation, not measured`, `upper bound`, `group_wait`, and does not contain `at least about 5 minutes` outside the sentence that quotes §6.
  - `test_every_number_comes_from_the_fixtures`: set the fixture's sustained rate, p95 pipeline delay, minimum time-to-page, lag median, per-key bytes, `idleness_s` and `timing_error_pct` to distinctive values (123,457; 98.7; 263.4; 23.9; 765,432; 61; 1.7) and assert each appears in the output, formatted.
  - `test_report_refuses_an_invalid_result`: `valid: false` in any file → an error naming the file and its reasons.
  - `test_report_refuses_data_that_is_not_synthetic`.
  - `test_battery_run_is_labelled`: `env.on_battery` true → the table says `on battery`.
  - `test_columns_split_when_environments_differ`: two commits among the three files → both appear.
  - `test_no_banned_words`: none of `spearheaded`, `leveraged`, `synergy`, `cutting-edge`, `results-driven`, `passionate` in `render(...)`.
- [ ] **Step 2: Run them.** `make test-py` → fail.
- [ ] **Step 3: Implement,** and add to the `Makefile`, with `performance` and `bench` in `.PHONY`:
  ```make
  # Renders docs/performance.md from docs/performance/*.json.
  performance:
  	cd py && uv run python -m tenant_baselines.bench_report --results ../docs/performance \
  		--out ../docs/performance.md

  # All three benchmarks (synthetic data), then the write-up. About 2 hours; AC power, lid open.
  bench: bench-throughput bench-delay bench-state performance
  ```
- [ ] **Step 4: Run the tests.** `make test-py && make lint-py` → clean.
- [ ] **Step 5: Commit.** `git add py/src/tenant_baselines/bench_report.py py/tests/test_bench_report.py Makefile && git commit -m "Render the performance write-up from the result files"`
- [ ] **Step 6: The sitting (about 2 hours).** Before it: the working tree is clean; `make test && make ci` are green on the branch tip; `pmset -g ps` says AC power; the lid stays open; `colima status` shows 8 CPUs and 10 GiB; no demo job is running (`make down && make up` if one is); the Flink web UI is closed; the broker's free space is at least twice what the pilot's `bytes_per_event` gives for one throughput fill and one delay history. Then one command:
  `make bench`
  Sleep prevention is the owner's choice: `KEEP_AWAKE=1 make bench` wraps each of the three drivers in `caffeinate -is`, and the default 0 touches nothing. It never stops a lid-close sleep.
  Expected: `throughput.json`, `delay.json` and `state.json` all `valid: true`; at least 54 of 60 incidents matched for `seasonal`/`k4`; `docs/performance.md` rendered. If a driver stops with an invalid run, read `invalid_reasons`, fix the condition (power, sleep, a job left running) and run that target again, then the ones after it. Do not edit a result file by hand, and do not publish a number from an invalid run.
- [ ] **Step 7: Read `docs/performance.md` end to end against the three JSON files.** Every figure in the text is in a file, and each statement of the list above is there.
- [ ] **Step 8: The rest of the docs.**
  - `README.md`: "milestone 5 of 6" in the status paragraph; one sentence under `## Design` linking `docs/performance.md` (throughput, alert delay and state size, measured on one laptop on synthetic data) and this plan. No performance number in the README; milestone 6 writes that block.
  - `docs/superpowers/m1-followups.md`, `m2-followups.md`, `m3-followups.md`: under each "Milestone 5" heading, mark every item `done (milestone 5)` or `moved to milestone 6`, as this plan's table says. Add to `m3-followups.md`'s "Milestone 6" list, one line each: the live job's retained checkpoints; the `live.*` topics' retention; the `PopulationBaselines` sweep; the `unit-tenants` removal; the demo generator's delivery timeout; one `make smoke` on `gen_live`'s loop, which now goes through `take_step`; the offset correction for `watermark-lag.sh`; the `make test-java` flake after a host sleep; moving or linking the extrapolation table into `docs/design.md`; a README performance block rendered from `docs/performance/`; the inconsistent `seconds` in `docs/results/check-paths.json`; and anything Tasks 3–6 found and did not fix, each with its number.
  - If the TaskManager's first memory reading was above 90% of its limit, or a run was OOM-killed, add "raise or re-split the TaskManager's memory" to the milestone 6 list with the reading. Otherwise the write-up's reading closes that follow-up.
- [ ] **Step 9: Check.** `make test && make ci` → green. `git grep -n -i -E 'spearheaded|leveraged|synergy|cutting-edge|results-driven|passionate' -- docs/performance.md README.md` → no output.
- [ ] **Step 10: Stop the stack.** `make down`. `docker ps`: if nothing is left, `colima stop`; if other containers are running, leave Colima up and say so in the report.
- [ ] **Step 11: Commit.** `git add docs/performance docs/performance.md README.md docs/superpowers/m1-followups.md docs/superpowers/m2-followups.md docs/superpowers/m3-followups.md && git commit -m "Milestone 5: throughput, delay and state size, measured and written up"`. Do not push. Then follow the owner's milestone workflow for the merge to `main`. Report: the three headline figures as `docs/performance.md` has them; the spread between the throughput runs and the `drop-ticks` share; matched and missed incidents, and the measured minimum time-to-page; which follow-ups were folded in and which moved; every builder's call made during the build that this plan did not already list; the hours spent against the budget line at the top; whether Colima was stopped; and that the GitHub Actions workflow has not run on GitHub.
