# tenant-baselines Milestone 3 Implementation Plan: alert noise control

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tenant alerts are held for two minutes and grouped by region before anything pages, unit status is published, Alertmanager routes regional and tenant pages apart and mutes tenant repeats under a regional alert, and the live watermark keeps moving through a total outage that spans a job restore.

**Architecture:** The live job reads Kafka with plain per-partition event time and unions a one-per-second wall-clock tick stream into it; the ticks are filtered out again, so only their watermark reaches `MinuteAggregator`. The `Scorer`'s output becomes a stream of alert *candidates*. A new `RegionGrouper`, keyed by region and fed both the candidates and the minute aggregates, holds each candidate, promotes to one regional alert or releases the tenant alert, resolves and re-holds, and writes drain and undrain events to a side output. Only the grouper's output reaches `<ns>.alerts`.

**Tech stack:** unchanged from milestone 2, plus `org.apache.flink:flink-connector-datagen:2.2.1` (scope `provided`; it is inside `flink-dist-2.2.1.jar` of the `flink:2.2.1-java17` image, checked on the running container, so it is not shaded into the job jar).

**Spec:** `docs/superpowers/specs/2026-11-09-tenant-baselines-design.md` (v2, approved). Milestone 3 is §8 item 3. Read §3, §4.2, §4.5, §4.7–§4.10, §5.5, §7 and §10 before starting. Follow-ups folded in come from `docs/superpowers/m2-followups.md` and `docs/superpowers/m1-followups.md`.

**Budget:** about 9 hours, against §8's "~4 h" for milestone 3. Task 1 (the watermark redesign, decided after §8 was written) about 2 h; Tasks 2–8 about 5 h; Tasks 9a and 9b about 2 h, most of it waiting on two full demo runs. The owner should read this figure before approving the plan.

**API checks done for this plan (2026-11-11, `ctx7`, no quota failures):**
- Flink `/apache/flink` at `release-2.2.0`:
  - `DataGeneratorSource(GeneratorFunction<Long, OUT>, long count, RateLimiterStrategy, TypeInformation<OUT>)` in `org.apache.flink.connector.datagen.source`; `RateLimiterStrategy.perSecond(double)` in `org.apache.flink.api.connector.source.util.ratelimit` limits the rate over all subtasks.
  - A union's event time is the minimum of its inputs; an input marked idle is left out.
  - A source is idle only when its main output *and* every split output are idle (`ProgressiveTimestampsAndWatermarks.IdlenessManager`).
  - A split's watermark starts at `Long.MIN_VALUE` and the source emits the minimum over its splits that are not idle (`CombinedWatermarkStatus`), so a split that has delivered nothing and is not yet idle holds the whole watermark.
  - The idleness timer runs on `WatermarkGeneratorSupplier.Context#getInputActivityClock`, which stops while the source is back-pressured (FLINK-35886, Flink 2.0).
  - `ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(fn, keySelector1, keySelector2, keyType)` returns a `KeyedTwoInputStreamOperatorTestHarness`.
- Alertmanager `/prometheus/alertmanager`: `matchers` on routes, `source_matchers`/`target_matchers`/`equal` on inhibit rules (a missing label equals an empty one); `amtool config routes test --verify.receivers=…` exits 1 on a mismatch. Run against the v0.34.2 container: the existing file passes `check-config` and routes `scope=region` to `regional-pager`.
- ClickHouse `/websites/clickhouse`: Kafka engine with `kafka_handle_error_mode = 'stream'`, `_error` and `_raw_message`; `ReplacingMergeTree` deduplicates by `ORDER BY`, read with `FINAL`.
- Kafka `/apache/kafka`: `kafka-topics.sh --create --config cleanup.policy=compact`; the broker rejects a record without a key on a compacted topic. `infra/kafka/create-topics.sh` already creates `<ns>.unit-status` this way.

## Global Constraints

- Everything in the milestone 1 and 2 plans' Global Constraints still applies, except the watermark lines replaced below.
- Java package root `io.github.sanchaypahalwani.tenantbaselines` (written `<pkg>`). Java builds and tests run only through `make` (`make test-java`, `make jar`); there is no host JDK. Python: `make test-py && make lint-py`.
- **Live watermark** *(owner's decision, 2026-11-11)*:
  - events source: `forBoundedOutOfOrderness(20 s).withIdleness(60 s)`, per Kafka partition;
  - tick source: 1 tick per second, parallelism 1, timestamp = wall clock, `forBoundedOutOfOrderness(20 s)`, no idleness;
  - `union`, then a filter that drops ticks, before `MinuteAggregator`;
  - live mode only. **Replay mode is not touched** (20 s bound, 30 s idleness, alignment).
- **Hold:** 2 minutes of event time. **Promote:** at the end of a hold, when the region has **≥ 3** active tenants and **≥ 50%** of them alert on the signal. **Regional resolve:** the share is **under 25%** for **5** minutes in a row. **Active tenant:** has a key in the region with **≥ 1200** requests over the last 60 minutes (an average of 20 a minute).
- **Grouping is per detector, setting and signal.** Alerts of different settings never count together (milestone 4 runs 12 settings side by side).
- **The grouper applies its inputs in event-time order, and in a fixed order within a minute.** It buffers candidates and activity changes and applies them in timers, so its output does not depend on how far one watermark step reaches (about 40 minutes of event time during the demo's backfill). Within one timer it sorts the minute's candidates by `(detector, setting, signal, key, state)` and walks groups and alerts in sorted key order, so the events of one minute come out in the same order on every run and §5.7's equivalence check can be a plain diff.
- **Builder's calls on §4.7's edge cases** (each with its reason; record them in `docs/design.md` in milestone 6):
  - A candidate that arrives while its group's regional alert fires is absorbed without a hold. *The regional page already covers it.*
  - A regional alert starts at the minute that promoted it, and its id is `AlertIds.of(detector, setting, "region", region, signal, startsAtMs)`. *A second regional alert over the same tenant alerts then gets a new id and its own row.*
  - A tenant counts as active for a signal while it has an alert on that signal that began when it was active. *Otherwise an outage empties its own denominator after an hour and the regional alert turns into tenant pages.*
  - A tenant with several alerting keys counts once. A tenant that is not active counts in neither the share's top nor its bottom, but its held alerts are absorbed with the rest.
  - Regional resolve looks only at the share; the ≥ 3 rule applies to forming. Consequence: with 3 or 4 active tenants, one tenant alert that lingers keeps the regional alert open (1 of 3 and 1 of 4 are not under 25%), and it goes on absorbing new candidates until that alert resolves.
  - A tenant alert absorbed by two regional alerts in turn writes two `absorbed` events with one `(alert_id, state)`; `tb.alert_log` keeps the later one. *The spec fixes that table's key (§4.9).*
  - A re-held alert that resolves during its fresh hold emits nothing. A `resolved` candidate the grouper has no alert for is passed through. *The alert was sent by an older job.*
- **Unit status** *(builder's calls on §4.8)*:
  - follows one setting, `seasonal` with the first `--seasonal-k`. *`tb.unit_status`'s key has no setting.*
  - only alerts that were **sent** (released after their hold) drain; held and absorbed ones don't;
  - a unit is dedicated when exactly one tenant sent it a request in the last 7 days of event time. *This is §4.1's definition, read from the data, with no inventory file.* That record is written unbuffered, so in a replay the unit-status output can differ between runs at the exact 7-day boundary; alerts are not affected.
  - no unit of a region is drained while that setting has a regional alert on any signal.
- **Alert events:** `state` is `firing`, `resolved` or `absorbed`. `scope` is `tenant` or `region`. A regional event has `key` = the region and `tenant` and `unit` = `""` (never null: ClickHouse's columns are not nullable). An absorbed event carries the tenant alert's own id and `reason` = `"regional:" + <regional alert id>`.
- **Output JSON never contains `NaN` or `Infinity`.**
- **State:** every operator has a `uid`. Existing state names and POJO classes stay. New state classes are Flink POJOs (public class, public no-arg constructor, public non-final fields), never a Kryo fallback.
- **Kafka record timestamps** stay at produce time.
- **Repo rules:** name no employers, clients, internal systems or companies; Apache-2.0; none of the words spearheaded, leveraged, synergy, cutting-edge, results-driven, passionate; every number in the README or the demo text comes from a script or a measurement; synthetic data is labelled synthetic; no claim of production use; commit locally after each task, never push, never create a GitHub repo.

## Open questions for the owner

Resolved. On 2026-11-11 the owner chose **A** (keep 60 s idleness, with the edits listed under A) and approved the plan for subagent-driven execution, with the budget of about 9 h and the three steady demo tenants.

1. **How far behind the wall clock the live watermark may run (§4.2, the 2026-11-11 decision).** The approved design is sound for the two problems it was chosen for; both are fixed (Task 1, checked end to end in Task 9a). One expected effect does not hold as stated. "Caught up: about a second behind the wall clock" is true only when every partition carries at least about one event a second. A partition that is not idle holds the watermark at its newest event − 20 s, so the watermark runs `20 s + the age of the newest event on the slowest non-idle partition` behind the wall clock, up to 80 s. In the demo world the quietest partition falls to 0.055 events/s on weekend nights (computed from `traffic.py`: an event every 18 s on average), so minutes there close some tens of seconds later than under milestone 2's latch. Nothing is dropped, and nothing stalls.
   - **A (planned):** keep 60 s. The spec amendment in Task 1 states the real bound; Task 9a widens `demo.sh`'s catch-up test to 180 s; Task 9b measures the lag (`scripts/watermark-lag.sh`) and records the weekday, hour and lag beside every page time. A is the default only with those edits.
   - **B:** shorten the idleness to 20–30 s. This halves the worst lag. The cost: a partition with unread history that delivers no record for that long (first fetch on a loaded Docker VM) is marked idle, the ticks carry the wall clock past it, and its history is dropped as late.
   - **C:** keep 60 s and give every demo partition steady traffic by reordering the world's keys. This hides the effect in the demo only.
   - The same 60 s also bounds the stall after every start, restore or failover: no minute closes until every partition has delivered an event or been silent for 60 s. B would shorten that stall too.
   - Milestone 5's fixed-rate benchmark keeps every partition busy, so its delay figures will see a lag of about 21 s; that write-up must say so.
   - The plan proceeds with A unless told otherwise; B is a two-constant change.

## Review Focus

1. **The job starts or restores while every partition is silent.** Expected: after 60 s the ticks carry the watermark, zero minutes close and the zero-traffic rule pages. Pinned in Task 1 (`ticksCloseMinutesWhenAnOpenSourceGoesSilent`, the only Java test where idleness, not end of input, hands over to the ticks) and Task 9a (smoke check 12).
2. **A region's traffic stops for more than an hour.** Every tenant drops under 20 requests a minute. Expected: the regional alert stays; it does not resolve into tenant pages. Pinned in Task 4 (`outageDoesNotEmptyItsOwnDenominator`).
3. **The end of a bounded replay with holds pending or a regional alert firing.** Expected: held alerts are released once, the job ends, no timer re-registers. Pinned in Task 4 (`finalWatermarkReleasesHeldAlertsAndStops`, with a timeout) and Task 6 (`PipelineTest`).
4. **A `resolved` candidate for an alert the grouper never saw** (state from an older checkpoint), **or an `absorbed` event reaching the forwarder.** Expected: the resolve is passed through; the forwarder ignores `absorbed` and a resolve without a firing. Pinned in Task 3 (`resolveForUnknownAlertPassesThrough`) and Task 7 (`test_absorbed_event_is_ignored`).
5. **Backfill and replay, where one watermark step covers many minutes** and candidates and aggregates of later minutes arrive before a hold's timer fires. Expected: the same alert log as minute-by-minute input. Pinned in Task 3 (`candidatesAheadOfTheWatermarkAreAppliedInTimeOrder`, `activityAheadOfTheWatermarkIsNotUsedEarly`).

## File map

```
flink-job/pom.xml                                                                  (modify, Task 1)
flink-job/src/main/java/<pkg>/
  model/Event.java  time/Watermarks.java  TenantBaselinesJob.java                  (modify, Task 1)
  time/Ticks.java                                                                  (new, Task 1)
  time/LiveWatermarkGenerator.java                                                 (delete, Task 1)
  detect/Alerts.java  detect/PopulationLookup.java                                 (new, Task 2)
  detect/Scorer.java                                                               (modify, Task 2)
  group/KeyActivity.java  ActivityFlip.java  TenantAlert.java  RegionalAlert.java  RegionGrouper.java  (new, Tasks 3–5)
  model/UnitStatusEvent.java                                                       (new, Task 5)
  JobConfig.java  Pipeline.java  TenantBaselinesJob.java  io/JsonSerialization.java  (modify, Task 6)
flink-job/src/test/java/<pkg>/...   one test class per new class; time/LiveWatermarkGeneratorTest.java is deleted
infra/clickhouse/init/01-schema.sql                                                (Task 6)
py/src/tenant_baselines/forwarder.py  scripts/check-am.sh (new)  Makefile  .github/workflows/ci.yml  (Task 7)
py/src/tenant_baselines/{world,traffic,incidents,gen_live}.py  py/tests/...        (Task 8)
scripts/{demo,smoke}.sh  scripts/watermark-lag.sh (new)  README.md  docs/superpowers/*.md  (Tasks 1, 8, 9a, 9b)
```

Budget: see the header (about 9 h). Tasks 1, 3, 4 and 5 touch watermark semantics or a stateful Flink operator and get the stronger implementer model; the rest don't need it.

---

### Task 1: Live watermark from per-partition event time and wall-clock ticks

**Implementer:** stronger model (watermark semantics; no operator state of its own).

**Files:**
- Modify: `flink-job/pom.xml`, `model/Event.java`, `time/Watermarks.java`, `TenantBaselinesJob.java`, `docs/superpowers/specs/2026-11-09-tenant-baselines-design.md`, `scripts/demo.sh` (one comment)
- Create: `time/Ticks.java`
- Delete: `time/LiveWatermarkGenerator.java`, `time/LiveWatermarkGeneratorTest.java`
- Test: `time/TicksTest.java` (new), `model/EventTest.java` (new), `TenantBaselinesJobTest.java`

**Interfaces (produces):**
- `Event.tick(long ts)` returns `new Event(ts, null, null, null, 0.0, true)`; `boolean isTick()` returns `tenant == null`. `EventJson.parse` never yields a null tenant, so no parsed event is a tick.
- `Watermarks`:
  - constants `BOUND = Duration.ofSeconds(20)`, `LIVE_IDLENESS = Duration.ofSeconds(60)`;
  - `events(Duration bound, Duration idleness)` = `forBoundedOutOfOrderness(bound).withIdleness(idleness).withTimestampAssigner((e, t) -> e.ts())`;
  - `live()` = `events(BOUND, LIVE_IDLENESS)`;
  - `ticks(Duration bound)` = `forBoundedOutOfOrderness(bound).withTimestampAssigner((e, t) -> e.ts())`, no idleness;
  - `replay()` unchanged.
- `final class Ticks`:
  - `static DataStream<Event> source(StreamExecutionEnvironment env)` = `source(env, 1.0, Watermarks.BOUND)`;
  - `static DataStream<Event> source(StreamExecutionEnvironment env, double perSecond, Duration bound)`: `env.fromSource(new DataGeneratorSource<>(i -> Event.tick(System.currentTimeMillis()), Long.MAX_VALUE, RateLimiterStrategy.perSecond(perSecond), TypeInformation.of(Event.class)), Watermarks.ticks(bound), "ticks").setParallelism(1).uid("ticks-source")`;
  - `static DataStream<Event> withTicks(DataStream<Event> events, DataStream<Event> ticks)` = `events.union(ticks).filter(e -> !e.isTick())` with uid and name `drop-ticks`.
- `TenantBaselinesJob.addTo`: in live mode the stream given to `Pipeline.build` is `Ticks.withTicks(events, Ticks.source(env))`; in replay mode it is `events`, as today.
- `pom.xml`: `flink-connector-datagen`, `${flink.version}`, scope `provided`, and `org.apache.flink:flink-connector-datagen` added to the shade excludes beside `flink-connector-base`.

**What was checked, so the implementer need not re-derive it:**
- Probed on Flink 2.2.1 with the project's own classpath: a tick (`null` tenant, region and unit) round-trips through the `PojoSerializer` Flink picks for `Event`, and `DataGeneratorSource` compiles and loads with the `provided` dependency. The source reports itself `BOUNDED` (it counts to `Long.MAX_VALUE`); the job still runs as a stream, because the runtime mode is `STREAMING` unless set otherwise. Don't set it to `AUTOMATIC`.
- The tick source keeps a small checkpointed state (its position in the number sequence). It needs the `uid` above; a checkpoint taken before it existed still restores, because a new operator starts empty. Tick timestamps come from the wall clock when the tick is made, so a restored position causes no burst of old ticks.
- The ticks never set the pace while Kafka has unread events: the union takes the minimum, and a channel that has not sent a watermark yet counts as `Long.MIN_VALUE`.
- After a start, a restore or a failover, no minute closes until every partition has delivered an event or has been silent for 60 s: a split that has delivered nothing and is not idle holds the watermark at `Long.MIN_VALUE` (checked in `CombinedWatermarkStatus`). This sets the restore timing in smoke checks 11 and 12 (Task 9a).
- One exposure stays, as in milestone 2: a partition that has been silent for 60 s is left out, and events written to it later with timestamps behind the watermark are dropped as late. `scripts/demo.sh` still submits the job only after every partition has data. Update its comment to say why in the new terms.
- The union stops the source from chaining to the next operator, so every event crosses one more local channel. Record it in `m2-followups.md` under milestone 5 (Task 9b).

**Spec amendments (exact text).** In §4.2, replace the whole "**Live:**" bullet with:

> - **Live:** the events source uses plain event time per partition: bounded out-of-orderness of 20 s, and a partition is marked idle after 60 s without an event. A second source emits one tick a second, stamped with the wall clock, with the same 20 s bound. The two streams are unioned and the ticks are filtered out before `MinuteAggregator`, so its watermark is the lower of the two.
>   - While the source is behind (backfill, or a backlog after a restore), that is pure event time.
>   - Once it has caught up, the watermark is the wall clock − 20 s, held back by the newest event of the slowest partition that is not idle: about a second more when every partition carries traffic, at most 60 s more when one is sparse (21 s to 80 s behind the wall clock in all).
>   - When every producer goes silent, the partitions go idle after 60 s and the ticks carry the wall clock − 20 s. This also holds after a restore or a failover during the silence. That matters, because a total outage is exactly when minutes must keep closing.
>   - After a start, a restore or a failover, no minute closes until every partition has delivered an event or has been silent for 60 s. A partition that has been silent for 60 s is left out until its next event; events written to it later with timestamps behind the watermark are dropped as late. `make demo` submits the job only after every partition has data.
>
>   *(Amended 2026-11-11 by the owner: this replaces the custom generator that latched to the wall clock. That one could not advance after a restore during a total outage, and it could hand over to the wall clock while a partition still had unread events.)*

In §3, after the Flink line of "Pinned versions", add: `- flink-connector-datagen **2.2.1** for the live tick source; it ships inside the Flink distribution. *(Added 2026-11-11.)*`

In §7's Java test list, replace "the live watermark generator, caught-up vs backfill;" with "the live watermark: ticks close minutes when events stop, and never outrun a backlog;".

- [ ] **Step 1: Write the failing tests**
  - `EventTest.tickIsNotAnEvent`: `Event.tick(5).isTick()` is true and its `ts()` is 5; `EventJson.parse` of `{"ts":1,"tn":"","rg":"r","un":"u","lat":1.0,"ok":true}` is not a tick.
  - `TicksTest` uses a `MiniClusterExtension` like `PipelineTest`'s and `env.setParallelism(2)`, so ticks cross a network channel and are serialized. Both tests run under `assertTimeoutPreemptively(Duration.ofSeconds(60), …)`.
    - `ticksCloseMinutesWhenTheEventsStop`: `M` = the wall-clock minute three minutes ago. Events: three events of key `a|eu|u1` at `M + 1 s`, `M + 2 s`, `M + 3 s`, from `env.fromData(…)` with `Watermarks.ticks(Duration.ofSeconds(1))` as their strategy. (This source is bounded: its end-of-input watermark, not idleness, hands over to the ticks. The next test covers idleness.) Ticks: `Ticks.source(env, 20, Duration.ofSeconds(1))`. Then `Ticks.withTicks(events, ticks).keyBy(Event::key).process(new MinuteAggregator("t", Long.MIN_VALUE, Duration.ofDays(7)))` and `executeAndCollect(2)`. Expected: `(minuteMs M, count 3)` then `(minuteMs M + 60_000, count 0)`, both with key `a|eu|u1`. The second minute has no event; only a tick's watermark can close it, and no tick was aggregated.
    - `ticksCloseMinutesWhenAnOpenSourceGoesSilent`: the events come from a source that never finishes, so no end-of-input watermark can close anything: a second `DataGeneratorSource` (`i -> new Event(M + 1_000, "a", "eu", "u1", 1.0, true)`, count `Long.MAX_VALUE`, `RateLimiterStrategy.perSecond(0.01)`, parallelism 1: one event at once, the next 100 s later) with strategy `Watermarks.events(Duration.ofSeconds(1), Duration.ofSeconds(2))`. Ticks and the rest as in the first test; `executeAndCollect(2)` → `(M, count 1)` then `(M + 60_000, count 0)`. Only the source going idle after 2 s lets the ticks close those minutes.
    - `ticksDoNotOutrunABacklog`: 180 events of one key, one a second from `MON = 345_600_000L` (three whole minutes, decades behind the wall clock), same wiring with `Watermarks.ticks(Duration.ofSeconds(20))` on both; `executeAndCollect(3)` → counts 60, 60, 60 for minutes `MON`, `MON + 60_000`, `MON + 120_000`.
  - `TenantBaselinesJobTest.everyOperatorOfTheLiveJobHasAUid`: the list also contains `ticks-source` and `drop-ticks`. New `replayJobHasNoTicks`: `uids(graph("--ns", "eval", "--mode", "replay"))` contains neither.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java` → compilation failure (`Event.tick`, `Ticks`).
- [ ] **Step 3: Implement.** Delete `LiveWatermarkGenerator` and its test. Apply the three spec amendments.
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures. `PipelineTest` is not touched by this task: `Pipeline.build` still takes an already timestamped stream. If `PipelineTest.aggregatesStopAtReplayEnd` fails, rerun once and report the failure either way; it failed once under load before this change.
- [ ] **Step 5: Check the jar.** `make jar`, then `unzip -l flink-job/target/tenant-baselines-job.jar | grep -c 'connector/datagen'` → `0`. (Task 9a's smoke run proves the class loads from the distribution.)
- [ ] **Step 6: Commit.** `git add -A && git commit -m "Drive the live watermark from per-partition event time and wall-clock ticks"`

### Task 2: Split the Scorer's alert builder and fallback lookup

Follow-ups from milestone 2. The grouper (Task 3) builds its events with the same factory.

**Implementer:** standard model. The Scorer's behaviour does not change.

**Files:**
- Create: `detect/Alerts.java`, `detect/PopulationLookup.java`
- Modify: `detect/Scorer.java`
- Test: `detect/AlertsTest.java`, `detect/PopulationLookupTest.java` (new)

**Interfaces (produces):**
- `final class Alerts`:
  - constants `FIRING = "firing"`, `RESOLVED = "resolved"`, `ABSORBED = "absorbed"`, `TENANT = "tenant"`, `REGION = "region"`;
  - `static AlertEvent of(String runId, String detector, String setting, String scope, String key, String tenant, String region, String unit, String signal, String state, long startsAtMs, long eventTimeMs, double value, double reference, String reason)`: `alertId = AlertIds.of(detector, setting, scope, key, signal, startsAtMs)`, `emittedAtMs = System.currentTimeMillis()`, value and reference through `finiteOrZero`;
  - `static double finiteOrZero(double v)`.
- `final class PopulationLookup`: `static String key(String scope, int bucket, long week)` and `static PopulationBaseline find(ReadOnlyBroadcastState<String, PopulationBaseline> state, String scope, int bucket, long week)`: the Scorer's `populationKey` and `lookup`, moved unchanged.
- `Scorer` calls both; its private `alert`, `finiteOrZero`, `populationKey` and `lookup` go.

- [ ] **Step 1: Write the failing tests**
  - `AlertsTest.nonFiniteValueAndReferenceBecomeZero`: `of(…, value NaN, reference POSITIVE_INFINITY, …)` has `value() == 0.0` and `reference() == 0.0`; `NEGATIVE_INFINITY` → 0.0; `0.25` stays `0.25`.
  - `AlertsTest.idIsTheDeterministicId`: `of("r", "seasonal", "k4", "tenant", "a|eu|u1", "a", "eu", "u1", "error_rate", "firing", 60_000, 240_000, 0.1, 0.03, null).alertId()` equals `AlertIds.of("seasonal", "k4", "tenant", "a|eu|u1", "error_rate", 60_000)`.
  - `PopulationLookupTest` (a small map-backed `ReadOnlyBroadcastState` fake): with records for weeks 4 and 5 of `("eu", 7)`, `find(…, 4)` is week 4's and `find(…, 5)` is week 5's; `find(…, 3)` is `null`; with only week 4 stored, `find(…, 6)` is week 4's; another bucket or scope → `null`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java` → compilation failure.
- [ ] **Step 3: Implement the two classes and switch the Scorer to them.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures; every existing Scorer test passes unchanged.
- [ ] **Step 5: Commit.** `git add flink-job && git commit -m "Move the alert factory and the fallback lookup out of the Scorer"`

### Task 3: RegionGrouper: activity, hold, promote and absorb

**Implementer:** stronger model (stateful Flink operator with timers).

**Files:**
- Create: `group/KeyActivity.java`, `group/ActivityFlip.java`, `group/TenantAlert.java`, `group/RegionalAlert.java`, `group/RegionGrouper.java`
- Test: `group/KeyActivityTest.java`, `group/RegionGrouperTest.java`

**Interfaces:**
- Consumes: `AlertEvent`, `MinuteAgg`, `Alerts`, `AlertIds`.
- Produces:
  - `class KeyActivity` (POJO): `public String tenant; public long lastMinuteMs = Long.MIN_VALUE; public int[] counts = new int[60]; public long sum; public boolean active;`
    - `void add(long minuteMs, long count)`: moves the ring on to `minuteMs` (skipped minutes count 0; a gap of 60 minutes or more clears it), stores the count (capped at `Integer.MAX_VALUE`), and sets `sum` and `active = sum >= ACTIVE_REQUESTS`.
    - constants `MINUTES = 60`, `ACTIVE_REQUESTS = 1200`.
  - `class ActivityFlip` (POJO): `public String tenant; public int delta;` (+1 when one of the tenant's keys becomes active, −1 when it stops).
  - `class TenantAlert` (POJO), every field public: `String status` (`"held"`, `"sent"` or `"absorbed"`), `long heldUntilMs`, `boolean activeAtFire`, and the candidate's `alertId, detector, setting, signal, key, tenant, unit, reason` (Strings), `startsAtMs` (long), `value, reference` (doubles).
  - `class RegionalAlert` (POJO): `public long startedAtMs; public int underStreak;`
  - `class RegionGrouper extends KeyedCoProcessFunction<String, AlertEvent, MinuteAgg, AlertEvent>`, keyed by region on both inputs:
    - constructor `(String runId, String liveDetector, String liveSetting, Duration expiry)` (the last three are used from Task 5 on);
    - constants `HOLD_MS = 120_000L`, `PROMOTE_SHARE = 0.5`, `RESOLVE_SHARE = 0.25`, `RESOLVE_AFTER = 5`, `MIN_ACTIVE_TENANTS = 3`.

**Why the inputs are buffered.** The grouper merges many keys and two inputs, and they arrive in no order between two watermarks. During the demo's backfill one watermark step covers about 40 minutes of event time (three weeks in about 150 s, a watermark every 200 ms), and the eval replays faster still. A decision at minute `m` must see the candidates and the activity up to `m` and nothing later; otherwise the alert log depends on how the watermark happened to step. So `processElement1` and `processElement2` only store, and everything is applied in `onTimer`, which Flink fires in timestamp order.

**State** (all scoped to the region by the key):
- `pending-alerts`: `MapState<Long, List<AlertEvent>>` and `pending-flips`: `MapState<Long, List<ActivityFlip>>`, both keyed by a timer time. (Checked on Flink 2.2.1: `TypeInformation.of(AlertEvent.class)` is a `PojoTypeInfo`, so `Types.LIST(…)` of it does not fall back to Kryo.)
- `alerts`: `MapState<String, TenantAlert>`, key `detector/setting/signal|key`. Entries exist from a candidate's `firing` to its `resolved`.
- `regional`: `MapState<String, RegionalAlert>`, key `detector/setting/signal` (the *group*). Entries exist while a regional alert fires.
- `activity`: `MapState<String, KeyActivity>`, by key. A key's minutes arrive in order, so its ring is updated at once; only the *flip* is buffered.
- `active-keys`: `MapState<String, Integer>`, tenant → number of its active keys in the region, changed only in `onTimer`.

**Time.** Every timer sits on the last millisecond of a minute: `minuteEnd(ts) = ts − floorMod(ts, 60_000) + 59_999`. A candidate decided at the end of minute `m` and that minute's aggregates both arrive with record timestamp `m + 59_999`. The decision time of a timer at `t` is `at = t + 1`.

```
processElement1(candidate c):
    t = minuteEnd(ctx.timestamp());  append c to pending-alerts[t];  register timer(t)

processElement2(minute m):
    k = activity.get(m.key) or new (tenant = m.tenant);  was = k.active
    k.add(m.minuteMs, m.count)
    if k.active != was:
        t = m.minuteMs + 59_999;  append ActivityFlip(m.tenant, k.active ? +1 : −1) to pending-flips[t]
                                          # not ctx.timestamp(): aggregates mode (milestone 4) may carry no record timestamp
        register timer(t)
    if k.sum == 0: activity.remove(m.key) else activity.put(m.key, k)

onTimer(t):   at = t + 1
  1. for each flip in pending-flips[t]: active-keys[tenant] += delta (remove the entry at 0);  remove pending-flips[t]
  2. for each candidate c in pending-alerts[t], sorted by (detector, setting, signal, key, state):  apply(c)
     remove pending-alerts[t]
  3. (Task 4: regional resolve)
  4. for each group, in sorted order, with a "held" alert whose heldUntilMs <= t (its alerts in sorted key order):
        (n, share) = share(group)
        if n >= 3 and share >= 0.5:
            r = RegionalAlert(startedAtMs = at − 60_000);  regional.put(group, r)
            emit Alerts.of(runId, detector, setting, "region", region, "", region, "", signal,
                           "firing", r.startedAtMs, at, share, 0.5, null)
            every "held" alert of the group, due or not → "absorbed";  emit absorbed(a, at)
        else:
            each due alert → "sent";  emit Alerts.of(… "tenant", a.key, a.tenant, region, a.unit,
                           a.signal, "firing", a.startsAtMs, at, a.value, a.reference, a.reason)
  5. (Task 5: unit status)   6. (Task 4: next minute's timer)

apply(c):
    group = c.detector/c.setting/c.signal;  id = group + "|" + c.key
    if c.state == "firing":
        if alerts has id: return                  # a repeat
        a = TenantAlert from c;  a.activeAtFire = active-keys has c.tenant
        if regional has group:
            a.status = "absorbed";  emit absorbed(a, c.eventTimeMs)        # no hold
        else:
            a.status = "held";  a.heldUntilMs = t + HOLD_MS;  register timer(a.heldUntilMs)
        alerts.put(id, a)
    else:                                         # resolved, any reason
        a = alerts.get(id);  alerts.remove(id)
        if a == null or a.status == "sent": emit c unchanged

share(group):
    A        = tenants in active-keys  ∪  tenants of the group's alerts with activeAtFire
    alerting = tenants of the group's alerts  ∩  A
    return (|A|, |A| == 0 ? 0 : |alerting| / |A|)

absorbed(a, at) = Alerts.of(… "tenant", a.key, …, "absorbed", a.startsAtMs, at, a.value, a.reference,
                            "regional:" + AlertIds.of(detector, setting, "region", region, signal, r.startedAtMs))
```

A released or absorbed event has the candidate's `alertId` (same inputs to `AlertIds`). The region is `ctx.getCurrentKey()`.

**Test harness:** `ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(new RegionGrouper("r", "seasonal", "k4", Duration.ofDays(7)), AlertEvent::region, MinuteAgg::region, Types.STRING)`. Helpers, all for region `eu`, with `min(n) = n * 60_000L`:
- `agg(tenant, unit, n, count)`: `processElement2` of that key's `MinuteAgg` for minute `n`, record timestamp `min(n) + 59_999`.
- `warmAt(n, tenants…)`: `agg(t, "u1", n, 1200)` for each; one minute of 1200 requests makes the tenant active for that minute and the next 59. `warm(tenants…)` = `warmAt(59, …)`.
- `fire(tenant, unit, signal, n)`: `processElement1` of a `firing` candidate (`seasonal`, `k4`, key `tenant|eu|unit`, `startsAtMs = min(n − 2)`, `eventTimeMs = min(n + 1)`), record timestamp `min(n) + 59_999`. `fire(detector, setting, …)` for other groups. `resolve(…)` likewise with state `resolved`.
- `wm(n)`: `processBothWatermarks(new Watermark(min(n) + 59_999))`.

- [ ] **Step 1: Write the failing tests**
  - `KeyActivityTest.activeAtTwelveHundredInTheHour`: 60 minutes of 20 → `active`; 59 of 20 and one of 19 → not. `oldMinutesLeaveTheHour`: 1200 at minute 0, then count 0 at minutes 1–59 → still active; count 0 at minute 60 → not, `sum == 0`. `gapClearsTheRing`: 1200 at minute 0, then count 5 at minute 200 → `sum == 5`.
  - `RegionGrouperTest`:
    - `tenantAlertIsReleasedAfterTwoMinutes`: `warm(a, b, c, d)`; `fire(a, u1, error_rate, 60)`; `wm(61)` → no output; `wm(62)` → exactly one event: `state "firing"`, `scope "tenant"`, the candidate's `alertId` and `startsAtMs` (3 480 000), `eventTimeMs == 3_780_000`.
    - `halfOfTheActiveTenantsFormOneRegionalAlert`: `warm(a, b, c, d)`; `fire(a, …, 60)`; `fire(b, …, 61)`; `wm(62)` → three events: one with `scope "region"`, `state "firing"`, `key "eu"`, `tenant ""`, `unit ""`, `region "eu"`, `signal "error_rate"`, `startsAtMs == 3_720_000`, `eventTimeMs == 3_780_000`, `value == 0.5`, `reference == 0.5`, `alertId == AlertIds.of("seasonal", "k4", "region", "eu", "error_rate", 3_720_000)`; and two with `state "absorbed"`, the candidates' ids, `eventTimeMs == 3_780_000` and `reason == "regional:" + that id`. `wm(64)` → nothing more: b never fires as a tenant alert.
    - `regionNeedsThreeActiveTenants`: `warm(a, b)`; both fire at 60; `wm(62)` → two tenant `firing` events, no regional.
    - `underHalfReleasesTenantAlerts`: `warm(a, b, c, d, e)`; a and b fire at 60 → `wm(62)` gives two tenant `firing` events.
    - `inactiveTenantsDoNotCount`: `warm(a, b, c)`; `agg(x, u1, 59, 600)`, the same for y and z; a, b and x fire at 60 → `wm(62)`: a regional alert with `value` ≈ 0.6667 and three `absorbed` events (x's too).
    - `tenantWithTwoKeysCountsOnce`: `warm(a, b, c, d)`; `fire(a, u1, …, 60)` and `fire(a, u2, …, 60)` → `wm(62)`: two tenant `firing` events (one tenant of four is 25%).
    - `alertsAlreadySentStaySent`: `warm(a, b, c, d)`; a fires at 60; `wm(62)` → a's tenant `firing`. b and c fire at 65; `wm(67)` → a regional `firing` with `value == 0.75` and `absorbed` for b and c only. Then `resolve(a, …, 75)`; `wm(75)` → a's `resolved` comes out unchanged.
    - `candidateDuringARegionalAlertIsAbsorbedWithoutAHold`: after the regional alert of the second test, `fire(d, u1, error_rate, 66)`; `wm(65)` → nothing; `wm(66)` → one `absorbed` event with `eventTimeMs == min(67)`.
    - `settingsAndSignalsAreGroupedApart`: `warm(a, b, c, d)`; a and b fire for `seasonal/k4` on `error_rate`, a alone for `seasonal/k6`, and c for `seasonal/k4` on `latency_p95`, all at 60 → `wm(62)`: one regional alert (`k4`, `error_rate`), two `absorbed`, and tenant `firing` events for a's `k6` alert and c's latency alert.
    - `resolveForUnknownAlertPassesThrough`: `resolve(a, u1, error_rate, 70)` with no state; `wm(70)` → the event comes out unchanged.
    - `absorbedAlertResolvesSilently`: after the regional alert of the second test, `resolve(b, …, 70)`; `wm(70)` → no output.
    - `candidatesAheadOfTheWatermarkAreAppliedInTimeOrder`: `warm(a, b, c, d, e)`; `fire(a, …, 60)` and `fire(b, …, 64)` with no watermark between; `wm(66)` in one step → exactly two tenant `firing` events, a's with `eventTimeMs == min(63)` and b's with `min(67)`. Then the same with four active tenants: a's tenant `firing` at `min(63)` (b's candidate is not there yet at minute 62: 1 of 4), then a regional `firing` at `min(67)` that absorbs b only.
    - `eventsOfOneMinuteComeOutInAFixedOrder`: `warm(a, b, c, d, e, f, g)` (three of seven is under half); c, a and b fire at 60 in that arrival order, each also for `seasonal/k6` → `wm(62)`: six tenant `firing` events ordered by setting (`k4` before `k6`), then key (a, b, c). The same input fed in the order b, c, a gives the same list.
    - `activityAheadOfTheWatermarkIsNotUsedEarly`: `warm(a, b)`; a and b fire at 60; `agg(c, u1, 63, 1200)`; `wm(63)` in one step → two tenant `firing` events at `min(63)` and no regional alert (c becomes active at minute 63, after the holds ended).
    - `stateTypesArePojos`: `TypeInformation.of(X.class)` is a `PojoTypeInfo` for `KeyActivity`, `ActivityFlip`, `TenantAlert`, `RegionalAlert`, `AlertEvent` and `MinuteAgg`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java` → compilation failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures.
- [ ] **Step 5: Commit.** `git add flink-job && git commit -m "Add RegionGrouper: hold tenant alerts, promote to a regional alert, absorb"`

### Task 4: RegionGrouper: regional resolve, fresh hold, end of input, restore

**Implementer:** stronger model (stateful Flink operator with timers).

**Files:**
- Modify: `group/RegionGrouper.java`
- Test: `group/RegionGrouperResolveTest.java`

**Interfaces:** no new signatures.

`onTimer(t)` gains its steps 3 and 6:

```
3. for each group in regional, in sorted order:
       (n, share) = share(group)
       r.underStreak = share < 0.25 ? r.underStreak + 1 : 0
       if r.underStreak >= 5:
           emit Alerts.of(… "region", region, "", region, "", signal, "resolved",
                          r.startedAtMs, at, share, 0.25, null);  regional.remove(group)
           each "absorbed" alert of the group → "held", heldUntilMs = t + HOLD_MS;  register timer
       else: regional.put(group, r)
6. if regional is not empty and ctx.timerService().currentWatermark() != Long.MAX_VALUE:
       register timer(t + 60_000)
```

So while a regional alert fires there is one timer a minute, the streak counts minutes, and at the final watermark of a bounded replay no timer re-registers. A fresh hold ends like any hold: step 4 promotes again or releases.

- [ ] **Step 1: Write the failing tests** (harness and helpers as in Task 3)
  - `regionalResolvesAfterFiveMinutesUnderAQuarter`: `warm(a, b, c, d)`; a and b fire at 60; `wm(62)` → regional. `resolve(b, …, 70)`: the share is 0.25, which is not under. `resolve(a, …, 72)`. `wm(75)` → no new output; `wm(76)` → one event: `scope "region"`, `state "resolved"`, the regional alert's id and `startsAtMs`, `eventTimeMs == 4_620_000`, `value == 0.0`, `reference == 0.25`. Neither a nor b ever produced a tenant event.
  - `shareBackAtAQuarterRestartsTheCount`: as above up to `wm(75)` (four minutes under); `fire(c, u1, error_rate, 76)`; `wm(80)` → one `absorbed` event for c and no `resolved`. `resolve(c, …, 81)`; `wm(84)` → nothing; `wm(85)` → `resolved` with `eventTimeMs == min(86)`.
  - `stillFiringAbsorbedAlertGetsAFreshHold`: `warm(a, b, c, d)`; a and b fire at 60; `wm(62)` → regional. `resolve(b, …, 63)`. `warmAt(64, e, f, g, h, i)`: the share is 1 of 9. `wm(67)` → no new output; `wm(68)` → the regional `resolved`, `eventTimeMs == 4_140_000`; `wm(69)` → nothing; `wm(70)` → a's tenant `firing` with its original `alertId` and `startsAtMs` and `eventTimeMs == 4_260_000`.
  - `reheldAlertThatResolvesStaysSilent`: the same up to `wm(68)`; `resolve(a, …, 69)`; `wm(72)` → no further output.
  - `reheldAlertCanJoinANewRegionalAlert`: `warm(a, b, c, d, e, f)`; a, b and c fire at 60; `wm(62)` → regional R1 (`value == 0.5`, `startsAtMs == min(62)`). `resolve(b, …, 63)` and `resolve(c, …, 63)`: 1 of 6. `wm(66)` → nothing new; `wm(67)` → R1 `resolved` (`eventTimeMs == min(68)`); a is held again until minute 69. `fire(b, …, 68)` and `fire(c, …, 68)` (new alerts, `startsAtMs == min(66)`). `wm(69)` → a regional `firing` R2 with `startsAtMs == min(69)`, `value == 0.5` and an id different from R1's, and three `absorbed` events: a's with its original id, b's and c's with their new ids, all with `reason == "regional:" + R2's id`. No tenant `firing` anywhere in the run.
  - `outageDoesNotEmptyItsOwnDenominator`: `warm(a, b, c)`; all three fire on `volume_5m` at 60; `wm(62)` → regional with `value == 1.0` and three `absorbed`. Then `agg(t, u1, n, 0)` for a, b and c for every minute 60–130 and `wm(130)` → no further output: no `resolved`, no tenant `firing`.
  - `finalWatermarkReleasesHeldAlertsAndStops` (`@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)`, as `ScorerExpiryTest.doesNotExpireAtFinalWatermark` has it; in same-thread mode a timer loop would hang the build instead of failing the test): `warm(a, b, c, d)`; a and b fire at 60; `wm(62)` → regional; d fires for `seasonal/k6` at 63; `processBothWatermarks(new Watermark(Long.MAX_VALUE))` → d's `k6` tenant `firing` comes out, the call returns, and `numEventTimeTimers() == 0`.
  - `stateSurvivesSnapshotAndRestore`: `warm(a, b, c, d)`; a fires at 60, b at 61, with no watermark yet (both still buffered); `snapshot(1, 1)`; a restored harness; `wm(62)` → the same three events as `halfOfTheActiveTenantsFormOneRegionalAlert`. `ProcessFunctionTestHarnesses` returns a harness that is already open, so build the restored one by hand, as `ScorerFallbackTest.restored()` does: `new KeyedTwoInputStreamOperatorTestHarness<>(new KeyedCoProcessOperator<>(fn), AlertEvent::region, MinuteAgg::region, Types.STRING)`, then `initializeState(snapshot)`, then `open()`.
- [ ] **Step 2: Run them.** `make test-java` → `outageDoesNotEmptyItsOwnDenominator`, `finalWatermarkReleasesHeldAlertsAndStops` and `stateSurvivesSnapshotAndRestore` pin behaviour Task 3's code already has and pass; the other five fail.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures, Task 3's tests included.
- [ ] **Step 5: Commit.** `git add flink-job && git commit -m "Resolve regional alerts and re-hold what they absorbed"`

### Task 5: Unit status

**Implementer:** stronger model (state in the same operator).

**Files:**
- Create: `model/UnitStatusEvent.java`
- Modify: `group/RegionGrouper.java`
- Test: `group/UnitStatusTest.java`

**Interfaces (produces):**
- `record UnitStatusEvent(String runId, String unit, String region, String status, String signal, long atMs, long emittedAtMs, String reason)`. `status` is `"drain"` or `"undrain"`. `signal` is the signal the unit is or was drained for. `reason` is `"dedicated"` or `"shared"` on a drain, `"cleared"` or `"regional"` on an undrain.
- `RegionGrouper.UNIT_STATUS = new OutputTag<UnitStatusEvent>("unit-status") {}`.

**State added:** `unit-tenants`: `MapState<String, Long>`, key `unit|tenant` → the latest minute with `count > 0`, written from `processElement2` when the entry is missing or more than an hour old. `drained`: `MapState<String, String>`, unit → signal.

`unit-tenants` is written when the minute arrives, not buffered: against a 7-day horizon a watermark step of minutes or hours changes nothing but the exact boundary, and it never changes an alert.

**Evaluation**, step 5 of `onTimer(t)`, with `live = liveDetector/liveSetting`:

```
regionalFiring = regional has any group that starts with live + "/"
want = {}                                             # unit → (signal, reason)
if not regionalFiring:
    for signal in (error_rate, latency_p95), in this order:
        tenantsByUnit = unit → set of tenants with a "sent" alert of group live/signal on that unit
        for (unit, tenants): if unit not in want:
            if |tenants| >= 2:                                  want[unit] = (signal, "shared")
            elif tenantsOf(unit, t) == 1:                       want[unit] = (signal, "dedicated")
for unit in drained but not in want:
    side-output UnitStatusEvent(runId, unit, region, "undrain", drained[unit], at, now,
                                regionalFiring ? "regional" : "cleared");  drained.remove(unit)
for unit in want but not in drained:
    side-output UnitStatusEvent(runId, unit, region, "drain", signal, at, now, reason);  drained.put(unit, signal)

tenantsOf(unit, t) = number of unit-tenants entries "unit|*" whose value > t − expiry
                     (entries at or under that are removed on the way)
```

Every change of a "sent" alert happens inside a timer (a release, or a buffered `resolved` being applied), so the evaluation always runs at the minute of the change and a unit gets at most one event per `at`. `volume_5m` never appears in the loop, so a traffic drop never drains.

- [ ] **Step 1: Write the failing tests** (Task 3's harness; `status()` = the values of `h.getSideOutput(RegionGrouper.UNIT_STATUS)`; "dedicated unit for a" = `agg(a, "u-d", 59, 1200)` with no other tenant on `u-d`)
  - `dedicatedUnitDrainsOnOneSentAlert`: `warm(b, c, d)`; dedicated unit for a; `fire(a, "u-d", error_rate, 60)`; `wm(61)` → no status; `wm(62)` → one event `{unit "u-d", region "eu", status "drain", signal "error_rate", atMs 3_780_000, reason "dedicated"}`. `resolve(a, "u-d", error_rate, 70)`; `wm(70)` → `{status "undrain", signal "error_rate", atMs 4_260_000, reason "cleared"}`.
  - `sharedUnitNeedsTwoTenantsOnTheSameSignal`: `warm(a, b, c, d, e)` (all on `u1`); a fires `error_rate` at 60 → after `wm(62)` no status; b fires `latency_p95` at 63 → after `wm(65)` no status; c fires `error_rate` at 66 → `wm(68)` gives `{unit "u1", status "drain", signal "error_rate", reason "shared"}` (two of five tenants on one signal is under half, so no regional alert forms).
  - `volumeAlertsNeverDrain`: dedicated unit for a; `warm(b, c, d)`; `fire(a, "u-d", volume_5m, 60)`; `wm(62)` → the tenant `firing`, no status.
  - `noDrainDuringARegionalAlert`: `warm(b, c, d)`; dedicated unit for a (four active tenants); a's `error_rate` alert on `u-d` at 60 → drain at `wm(62)`. b and c fire `error_rate` at 65 → `wm(67)`: a regional alert (3 of 4) and `{unit "u-d", status "undrain", reason "regional", atMs min(68)}`. `resolve(b, …, 70)`, `resolve(c, …, 70)`: the share is 1 of 4, not under a quarter, so the regional alert stays and there is no status through `wm(80)`. `warmAt(81, e, f, g, h)` (1 of 8) → the regional alert resolves at `wm(85)`, and the same timer drains `u-d` again (`reason "dedicated"`, `atMs min(86)`), because a's alert was sent and still fires.
  - `onlyTheLiveSettingDrains`: dedicated unit for a; `warm(b, c, d)`; `fire("seasonal", "k6", a, "u-d", error_rate, 60)` and `fire("global_static", "default", a, "u-d", error_rate, 60)`; `wm(62)` → two tenant `firing` events, no status.
  - `unitWithOneTenantInSevenDaysIsDedicated`: `agg(a, "u2", 59, 1200)` and `agg(b, "u2", 59, 5)`; `warm(c, d)`; `fire(a, "u2", error_rate, 60)` → after `wm(62)` no status (two tenants). Fresh harness with everything but b shifted by 8 days (`D = 11_520` minutes): `agg(b, "u2", 0, 5)`, `agg(a, "u2", D + 59, 1200)`, `warmAt(D + 59, c, d)`, `fire(a, "u2", error_rate, D + 60)` → `wm(D + 62)` drains `u2` with `reason "dedicated"`.
  - `drainSurvivesSnapshotAndRestore`: drained as in the first test; snapshot, then a restored harness built by hand as in Task 4 (`initializeState(snapshot)` before `open()`); the resolve and `wm(70)` then give the `undrain`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures.
- [ ] **Step 5: Commit.** `git add flink-job && git commit -m "Publish unit drain and undrain events from the RegionGrouper"`

### Task 6: Job wiring and `tb.unit_status`

**Implementer:** standard model.

**Files:**
- Modify: `JobConfig.java`, `Pipeline.java`, `TenantBaselinesJob.java`, `io/JsonSerialization.java`, `infra/clickhouse/init/01-schema.sql`
- Test: `JobConfigTest.java`, `io/JsonSerializationTest.java`, `PipelineTest.java`, `TenantBaselinesJobTest.java`

**Interfaces:**
- Consumes: `RegionGrouper`, `UnitStatusEvent`.
- Produces:
  - `JobConfig.unitStatusTopic()` = `ns + ".unit-status"`.
  - `Pipeline.Outputs(DataStream<MinuteAgg> aggregates, DataStream<AlertEvent> alerts, DataStream<ScoreEvent> scores, DataStream<UnitStatusEvent> unitStatus, DataStream<Event> late)`. `alerts` is now the grouper's output.
  - In `Pipeline.build`, after the Scorer (`candidates`):
    ```
    grouped = candidates.keyBy(AlertEvent::region)
                  .connect(aggregates.keyBy(MinuteAgg::region))
                  .process(new RegionGrouper(cfg.runId(), Scorer.SEASONAL,
                                             cfg.seasonal().get(0).setting(), KEY_EXPIRY))
                                                                      uid and name region-grouper
    alerts = grouped;  unitStatus = grouped.getSideOutput(RegionGrouper.UNIT_STATUS)
    ```
  - `JsonSerialization.unitKey()`: `SerializationSchema<UnitStatusEvent>` of the UTF-8 `unit` (the compacted topic needs a key).
  - `TenantBaselinesJob.addTo` sinks `unitStatus` to `cfg.unitStatusTopic()`, uid and name `unit-status-sink`, at least once.
- **ClickHouse** (added to `01-schema.sql`, same patterns as `scores`):
  - `tb.kafka_unit_status`: `run_id, unit, region, status, signal String; at_ms, emitted_at_ms Int64; reason String`; topics `live.unit-status,eval.unit-status,bench.unit-status`; group `ch-unit-status`.
  - `tb.unit_status`: `ReplacingMergeTree ORDER BY (ns, run_id, unit, at)` with `ns, run_id, unit, region, status, signal, reason` as `LowCardinality(String)` and `at`, `emitted_at` as `DateTime64(3, 'UTC')`.
  - `tb.mv_unit_status` and `tb.mv_unit_status_errors`.
  - `tb.alert_log` needs no change: `state` already takes `absorbed`.
- **Grafana:** no change. The alert-log panel already shows `state` and `scope`; the unit-status panel is milestone 6 (§4.10).

- [ ] **Step 1: Write the failing tests**
  - `JobConfigTest.defaults` also asserts `unitStatusTopic() == "live.unit-status"`.
  - `JsonSerializationTest.unitStatusEventKeysAndKey`: the JSON has exactly the keys `run_id, unit, region, status, signal, at_ms, emitted_at_ms, reason`; `unitKey()` gives the unit's bytes. `regionalAlertWritesEmptyTenantAndUnit`: a regional `AlertEvent` serializes `"tenant":""` and `"unit":""`, not `null`.
  - `PipelineTest`:
    - `errorSpikeProducesOneFiringAlert` still passes, and now also asserts `eventTimeMs == 900_000` (decided at 780 000, held two minutes) and `scope == "tenant"`.
    - `seasonalAlertFiresAfterTwoWeeksOfHistory` still passes unchanged.
    - New `regionalAlertAbsorbsTenantAlerts`: four tenants `t1`…`t4` on unit `u1` in region `eu`, 100 events a minute each for minutes 0–74; from minute 65, every second event of `t1`, `t2` and `t3` is an error; `replayEndMs = 75 * MINUTE_MS`. Build the list minute by minute (minute in the outer loop, tenants inside, as `events()` does): `withTimestamps` uses `forMonotonousTimestamps`, and a list built tenant by tenant would drop three tenants as late. Among the `global_static` events: exactly one with `scope "region"` and `state "firing"`, three with `state "absorbed"`, and none with `scope "tenant"` and `state "firing"`. (Minutes 0–59 make all four tenants active; three of four alert.)
  - `TenantBaselinesJobTest.everyOperatorOfTheLiveJobHasAUid`: the list also contains `region-grouper` and `unit-status-sink`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests.** `make test` → 0 failures, ruff clean. The grouper adds one keyed operator to every `PipelineTest` job. If `aggregatesStopAtReplayEnd` fails, rerun once and report it either way.
- [ ] **Step 5: Check the schema on a fresh stack.**
  Run: `make reset && make jar && make up` (`make reset` deletes the volumes of the demo stack that is running now; that is intended, the schema file only runs on an empty volume)
  Run: `curl -s -u tb:tb-local 'http://localhost:8123/?query=SHOW%20TABLES%20FROM%20tb'`
  Expected: the milestone 2 tables plus `kafka_unit_status`, `unit_status`, `mv_unit_status`, `mv_unit_status_errors`.
- [ ] **Step 6: Commit.** `git add -A && git commit -m "Wire the RegionGrouper and unit status through the job and ClickHouse"`

### Task 7: Forwarder and Alertmanager

`infra/alertmanager/alertmanager.yml` already has the two receivers, the `scope` routes and the inhibit rule (written in milestone 1, never exercised). This task proves the routing in CI and pins the forwarder's handling of the new events. The inhibit rule itself is proven in Task 9a against a running Alertmanager.

**Implementer:** standard model.

**Files:**
- Modify: `py/src/tenant_baselines/forwarder.py`, `Makefile`, `.github/workflows/ci.yml`, the spec (§7)
- Create: `scripts/check-am.sh`
- Test: `py/tests/test_forwarder.py`

**Interfaces (produces):**
- `forwarder.build_parser() -> argparse.ArgumentParser` with today's five options and defaults; `main` calls it.
- `scripts/check-am.sh`: runs `amtool` from `prom/alertmanager:v0.34.2` (`docker run --rm -v "$PWD/infra/alertmanager":/cfg:ro --entrypoint amtool …`) and exits non-zero unless all of these hold:
  - `check-config /cfg/alertmanager.yml` succeeds and its output contains `1 inhibit rules`;
  - `config routes test --config.file=/cfg/alertmanager.yml --verify.receivers=regional-pager alertname=RegionAnomaly scope=region region=ap-southeast signal=error_rate` succeeds;
  - the same with `--verify.receivers=tenant-pager alertname=TenantAnomaly scope=tenant region=ap-southeast tenant=t09 signal=error_rate` succeeds.
- `make check-am` runs the script; `make ci` runs it too; `ci.yml` gets a third job `alertmanager-config` that checks out the repo and runs `scripts/check-am.sh`.

**Spec amendment (exact text).** In §7, replace "The GitHub Actions workflow has two jobs: `mvn verify` on Temurin 17, and `uv sync` + ruff + pytest." with: "The GitHub Actions workflow has three jobs: `mvn verify` on Temurin 17, `uv sync` + ruff + pytest, and an Alertmanager config check with `amtool`. *(Amended 2026-11-11, builder's call, for milestone 3.)*"

- [ ] **Step 1: Write the tests**
  - `test_build_parser_defaults`: `build_parser().parse_args([])` has `bootstrap == "localhost:9092"`, `topic == "live.alerts"`, `alertmanager == "http://alertmanager:9093"`, `live_detector == "seasonal"`, `live_setting == "k4"`. (Fails until `build_parser` exists.)
  - `test_absorbed_event_is_ignored`: an `absorbed` event for an unknown id → `due()` is empty; an `absorbed` event with the id of a firing alert leaves that alert firing, with no `endsAt`.
  - `test_regional_alert_entry`: `to_am_alert` of an event with `scope "region"`, `key "eu-central"`, `tenant ""`, `unit ""`, `region "eu-central"` has labels exactly `{alertname: RegionAnomaly, scope: region, region: eu-central, signal, detector, setting}` and the summary `error_rate for eu-central / all units`.
  - The last two pin behaviour the forwarder already has; they are expected to pass before Step 3.
- [ ] **Step 2: Run them.** `make test-py` → `test_build_parser_defaults` fails; the other two pass.
- [ ] **Step 3: Implement `build_parser`, the script, the Makefile target and the CI job, and apply the spec amendment.**
- [ ] **Step 4: Run.** `make test-py && make lint-py` → all pass. `make check-am` → exit 0. Then change `regional-pager` to `tenant-pager` in the script's first route test, run it, see exit 1, and change it back.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Check the Alertmanager routes in CI and pin the forwarder's handling of grouped alerts"`

### Task 8: Demo world and script that always form a regional alert

**Why.** Computed from `world.py` and `traffic.py` at every quarter-hour of a week, counting a tenant as active when one of its keys has a mean rate of at least 20 a minute over the 60 minutes before (one sample a minute): with today's 8 tenants, no region has 3 active tenants in 290 of 672 quarter-hours (weekend and night load is 5% to 17% of peak), so no spike-region rule can guarantee a regional alert. Tenants that follow working hours would need about 500 requests a minute of peak per key to stay active on a weekend night, which triples the history. Three small tenants with a flat profile do it for 3.6 million more events over three weeks (20.7 → 24.3 million, computed). With them, `ap-southeast` has 3 to 5 active tenants in every quarter-hour, and at least 60% of the active ones have a key at 25 requests a minute or more. These are this plan's own computations; none of them goes into the README unless a script in the repo prints it.

**Implementer:** standard model.

**Files:**
- Modify: `py/src/tenant_baselines/world.py`, `traffic.py`, `incidents.py`, `gen_live.py`, the spec (§4.10)
- Test: `py/tests/test_world.py`, `test_traffic.py`, `test_incidents.py`, `test_gen_live.py`

**Interfaces (produces):**
- `Tenant` gains `steady: bool = False`. `traffic.load(tenant, t_ms)` returns `1.0` for a steady tenant.
- `build_world("demo", seed)`: the 8 tenants, their units and their 18 keys stay exactly as they are, in the same order (same random draws, so the same key → partition mapping). Appended after them, with fixed values and no random draw:
  - tenants `t09` (`peak_rpm` 60.0), `t10` (30.0), `t11` (30.0): home region `ap-southeast`, `tz_offset_h` 8, `steady=True`;
  - unit `ap-southeast-d-t09`, dedicated for `t09`, `base_latency_ms` 55.0;
  - keys, in this order: `t09|ap-southeast|ap-southeast-s1`, `t09|ap-southeast|ap-southeast-d-t09`, `t10|ap-southeast|ap-southeast-s1`, `t11|ap-southeast|ap-southeast-s1`. Every steady key runs at 30 requests a minute.
- `incidents.pick_drop_key(world, t_ms)`: as today, among tenants that are not steady. *The drop shows a seasonal volume baseline, and it must not land on the burst key.*
- `incidents.pick_burst_key(world) -> Key`: the first key in world order on a dedicated unit of a steady tenant.
- `incidents.pick_spike_region` is deleted. The spike region is the burst key's region.
- `incidents.demo_script(world, t0_ms) -> list[Incident]`, three incidents, minutes after `t0`:
  1. `Incident("volume_mult", t0 + 2, t0 + 12, 0.1, key=pick_drop_key(world, t0 + 2).key)`: the drop, unchanged.
  2. `Incident("error_spike", t0 + 3, t0 + 21, 0.20, key=pick_burst_key(world).key)`: the burst on one tenant's dedicated unit. It pages as a tenant alert at about `t0 + 9` and drains the unit.
  3. `Incident("error_spike", t0 + 11, t0 + 21, 0.20, region=<burst key's region>)`: the regional spike. The other tenants' candidates arrive from about `t0 + 14`; the first hold ends at about `t0 + 16` with at least 2 of 3 active tenants alerting, so a regional alert forms, absorbs them, undrains the unit, and Alertmanager mutes the burst's tenant alert. In an estimated 1–3% of runs `t10` or `t11` pages as a tenant alert first, when their alerts land a minute apart with five tenants active; the regional alert still forms.
- `gen_live`'s log line becomes: `caught up after <s> s, <n> events; demo script: volume x0.1 on <key> <from>-<to> UTC, error burst on <key> <from>-<to> UTC, error spike in <region> <from>-<to> UTC`. `scripts/demo.sh` parses `volume x0.1 on <key> `, which stays.

**Spec amendment (exact text).** In §4.10, replace the bullet "After catch-up it injects a scripted sequence: …" with:

> - After catch-up it injects a scripted sequence: a dedicated-unit tenant drop; an error burst on one tenant's dedicated unit, which pages and drains the unit; then an error spike across that tenant's region, which forms one regional alert. It prints the URLs and what to watch for. The demo world has three small tenants with a flat traffic profile in that region, so a regional alert can form at any hour; without them no region has three active tenants on nights and weekends. *(Amended 2026-11-11, builder's call, for milestone 3.)*

- [ ] **Step 1: Write the failing tests**
  - `test_world.py::test_demo_world_shape`: 11 tenants, 22 keys, 3 dedicated units; exactly `t09`, `t10`, `t11` are steady.
  - `test_world.py::test_first_eight_tenants_are_unchanged`: `[t.peak_rpm for t in w.tenants[:8]] == [197.8, 289.4, 195.4, 52.9, 309.5, 214.4, 97.6, 238.8]`; `w.keys[0].key == "t01|us-east|us-east-s1"`, `w.keys[17].key == "t08|eu-central|eu-central-s2"`, `w.keys[19].key == "t09|ap-southeast|ap-southeast-d-t09"`.
  - `test_traffic.py::test_steady_tenant_has_flat_load`: for a steady tenant, `key_rate_rpm(t09, 2, ms) == 30.0` at a weekday local noon and at a Sunday 03:00 local.
  - `test_gen_live.py::test_partition_of_covers_all_partitions` still passes with 22 keys.
  - `test_incidents.py::test_demo_script_shape`: three incidents. The drop is as before, and its key's tenant is not steady. The burst is an `error_spike` of 0.20 on a key whose unit is dedicated to a steady tenant, from `t0 + 3 min` to `t0 + 21 min`. The spike is an `error_spike` of 0.20 with `region` equal to the burst key's region, from `t0 + 11 min` to `t0 + 21 min`.
  - `test_incidents.py::test_demo_script_targets_are_detectable_all_week` (replaces the current body; its docstring says what it proves: the rates and shares *allow* each alert and a regional alert at every quarter-hour, not that the detector fires; `t0` at every quarter-hour of the week from `1_767_225_600_000`). With `active(region, t)` = tenants having a key in the region whose mean `key_rate_rpm` over the 60 minutes before `t` is ≥ 20, and `strong` = active tenants with a key at ≥ 25 at both the spike's start and end:
    - the drop key's rate at the drop's start is ≥ 7;
    - the burst key's rate is ≥ 25 at the burst's start and end;
    - at `t0 + 8 min` (the burst's hold ends): `len(active) >= 3`, so one tenant is under half;
    - at `t0 + 16 min` and `t0 + 19 min`: `len(active) >= 3`, `len(strong) >= 3` and `2 * len(strong) >= len(active)`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-py`
- [ ] **Step 3: Implement.** Apply the spec amendment.
- [ ] **Step 4: Run tests and lint.** `make test-py && make lint-py` → all pass. `git grep -n pick_spike_region` → no matches.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Give the demo world three steady tenants and script a burst before the regional spike"`

### Task 9a: Smoke test on the grouped demo

**Implementer:** standard model; the debugging, if a check fails, wants the stronger one.

**Files:**
- Create: `scripts/watermark-lag.sh`
- Modify: `scripts/demo.sh`, `scripts/smoke.sh`

**Behaviour:**
- **`scripts/watermark-lag.sh [samples] [interval_s]`** (defaults 60 and 5): like `late-events.sh`, reads the running job's metrics whose id ends in `.minute-aggregator.currentInputWatermark`, takes the lowest per sample, and prints `min`, `median` and `max` of `now − watermark` in seconds, and the UTC weekday and hour. Exits 1 when there is no running job or no such metric.
- **`scripts/demo.sh`:**
  - `CAUGHT_UP_S=180` (now 120), with a comment: the newest closed minute can start up to 60 s + 80 s of watermark lag before now, so 120 passes at quiet hours only by luck.
  - The "What to watch" text names the three incidents, hold included, and says the regional alert *usually* absorbs the other tenants' alerts (one of them can page first when their alerts land a minute apart). It gives no page times until Task 9b has measured them.
- **`scripts/smoke.sh`** (still after `make demo`; `LIMIT_S` stays 1800).
  - A `CHECKS` environment variable, a space-separated list of check numbers, default all: only those run and count, and the final line reports how many ran. `CHECKS="11 12" scripts/smoke.sh` reruns the restore and outage checks on a live demo stack.
  - The header says that check 5 can be observed only during the scripted spike, from about 17 to 26 minutes after catch-up, so it cannot be rerun later on the same stack.
  - With `LIVE = dateDiff('second', event_time, emitted_at) < 600`. Polled checks:
    1. `tb.aggregates` reaches back 20 days (as today).
    2. A seasonal `volume_5m` tenant alert on a dedicated unit, `scope='tenant' AND state='firing'`, decided live (as today, plus the scope).
    3. **A regional alert formed:** `tb.alert_log FINAL` has `scope='region' AND state='firing' AND detector='seasonal' AND signal='error_rate' AND LIVE`.
    4. **Tenant alerts were absorbed:** a row with `state='absorbed' AND detector='seasonal' AND signal='error_rate' AND reason LIKE 'regional:%' AND LIVE`.
    5. **The inhibit works:** `GET :9093/api/v2/alerts` returns an alert with `labels.scope == "tenant"`, `labels.signal == "error_rate"` and a non-empty `status.inhibitedBy`.
    6. **Both receivers were paged:** `/data/notifications.jsonl` in `pager-sink` has a line with `"receiver": "/regional"` and one with `"receiver": "/tenant"`.
    7. `tb.scores` has a median for each signal (as today).
    8. **Unit status rows appear:** `tb.unit_status FINAL` has, for one `unit LIKE '%-d-%'` with `ns='live'` and `dateDiff('second', at, emitted_at) < 600`, a `drain` row and an `undrain` row with a later `at`.
  - Then, once, in this order:
    9. `tb.ingest_errors` is empty.
    10. `lateEvents` is 0: the history → live seam (milestone 2's check).
    11. The restore check, with the fix from the follow-ups: after the restore from the oldest retained checkpoint, re-emitted minutes exist, and each of these is 0:
        - `count() - uniqExact(key, minute)` on `tb.aggregates FINAL`;
        - `count() - uniqExact(key, signal, minute)` on `tb.scores FINAL`;
        - `count() - uniqExact(detector, setting, scope, key, signal, starts_at, state)` on `tb.alert_log FINAL` (business columns, not the dedup key `alert_id`, so a changed id shows up);
        - the restored job's `lateEvents` (it restarts on a backlog at least 60 s deep while traffic is live).

        After a restore no minute closes until every partition has delivered an event or been silent for 60 s (Task 1), so the settle time before reading is 120 s, not today's 60.
    12. **A total outage across a restore still pages.** `docker compose --profile demo stop generator`; note `stop = now`; sleep 90 s, so every partition is silent; `submit-job.sh` (restore from the newest checkpoint). Poll for up to 900 s until both hold:
        - `tb.aggregates` has a `ns='live'` row with `count = 0 AND minute >= stop + 5 minutes`;
        - `tb.alert_log FINAL` has a `state='firing' AND detector='seasonal' AND signal='volume_5m' AND event_time > stop AND LIVE` row.

        On milestone 2's watermark this check never passes: the watermark stays at the last event. Expected time from `stop`: about 7 to 11 minutes (a warm key's 5-minute sum breaches after about 2 zero minutes, fires after about 4 and is released after about 6 of event time; the restored job needs 60 s of silence before the ticks take over).

`make smoke` now takes about 50 minutes. `make reset` deletes the volumes of whatever demo stack is running; that is intended here.

- [ ] **Step 1: Run the full test suite.** `make test` → Java and Python 0 failures, ruff clean.
- [ ] **Step 2: Run CI locally.** `make ci` → the Java job, the Python job and `check-am` exit 0.
- [ ] **Step 3: Write the scripts and check their syntax.** `bash -n scripts/smoke.sh scripts/demo.sh scripts/watermark-lag.sh` → no output.
- [ ] **Step 4: Run the smoke test from empty volumes.**
  Run: `make reset && make smoke`
  Expected: `PASS` for all 12 checks. On a FAIL, debug with superpowers:systematic-debugging before changing code. If only check 11 or 12 failed, fix and rerun with `CHECKS="11 12" scripts/smoke.sh` (12 needs the generator running again: `docker compose --profile demo start generator`, then wait two minutes). If check 5 alone fails, read `status` of the tenant alert in `GET :9093/api/v2/alerts` and the regional alert's labels before touching the rule; it needs a full rerun.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Smoke-test regional grouping, the inhibit, unit status and an outage across a restore"`

### Task 9b: Measurements, README and follow-ups

**Implementer:** standard model.

**Files:**
- Modify: `scripts/demo.sh`, `README.md`, `docs/superpowers/m2-followups.md`, `docs/superpowers/m1-followups.md`

**Behaviour:**
- **`README.md`:** "milestone 3 of 6"; two sentences on the hold, the regional alert and unit status; the measured warm-up and page times from Step 2, each with the machine, the Docker VM, the UTC weekday and hour of the run and the watermark lag measured on that run; one sentence that the page times include the watermark lag, which is between 21 s and 80 s depending on how busy the quietest partition is; one sentence that three demo tenants have a flat synthetic profile so a regional alert can form at any hour; a link to this plan. No figure from this plan's own computations (event counts, quarter-hours) goes into the README unless a script in the repo prints it.
- **`m2-followups.md`:** delete the whole "For the owner, at the start of milestone 3" and "Milestone 3" sections. Add under "Milestone 5":
  - the union adds one local channel hop per event, to be measured in the benchmark;
  - the fixed-rate benchmark keeps every partition busy, so its delay figures see a watermark lag of about 21 s; the write-up must say that a sparse partition adds up to 60 s;
  - a tenant alert absorbed by two regional alerts keeps only the later `absorbed` row in `tb.alert_log`.
- **`m1-followups.md`:** delete "Watch `lateEvents` across restarts…" (smoke check 11 does) and the test items "watermark latch boundaries" and "`Watermarks` factories" (the latch is gone).

- [ ] **Step 1: Measure the watermark lag.** On a fresh run (`make reset && make demo`; this deletes the running stack's volumes, as intended), after catch-up: `scripts/watermark-lag.sh 60 5` → record min, median, max and the UTC weekday and hour. Expected from the model, not a pass mark: about 21 to 25 s when every partition is busy, up to 80 s when one is sparse.
- [ ] **Step 2: Record the measurements** on the same run (each reported as measured on this machine, with the Colima size, the UTC weekday and hour, and Step 1's median lag beside it):
  - warm-up seconds, the generator's catch-up seconds and event count (expected about 24.3 million);
  - from `pager-sink`'s `received_at` and the generator's `demo script:` line: tenant drop start → tenant page; burst start → tenant page; regional spike start → regional page. Each now includes the 2-minute hold and the watermark lag; milestone 2 measured 4.4 and 3.4 minutes without the hold;
  - `lateEvents` at catch-up; the latest checkpoint's size; `docker stats --no-stream` for the TaskManager (under 2560m) and `docker inspect -f '{{.RestartCount}} {{.State.OOMKilled}}'` (expected `0 false`);
  - from Task 9a's smoke output: check 12's time from `stop` to the alert.
  - If warm-up took more than 5 minutes, stop here and report the figures; do not tune.
- [ ] **Step 3: Put the measured figures into `demo.sh` and the README.** Every number in either comes from Step 1 or 2.
- [ ] **Step 4: Check by hand in the browser.** The alert log shows `absorbed` rows and a `region` row; Alertmanager shows the regional alert, and the burst's tenant alert as inhibited; `SELECT * FROM tb.unit_status FINAL ORDER BY at` shows the drain and the undrain.
- [ ] **Step 5: Do the repo hygiene check.**
  Run: `git grep -n -i -E 'spearhead|leverag|synerg|cutting-edge|results-driven|passionate'`
  Expected: no matches outside `docs/superpowers/`.
- [ ] **Step 6: Commit and stop.**
  `git add -A && git commit -m "Milestone 3: alert noise control and a watermark that survives a silent restore"`
  Then stop for the owner's review. Report the smoke output and the Step 1 and 2 measurements with their weekday and hour, the hours spent against the budget line at the top of this plan, and say plainly that the GitHub Actions workflow hasn't run on GitHub.
