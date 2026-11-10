# tenant-baselines Milestone 2 Implementation Plan: seasonal baselines

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A seasonal detector (per-key hour-of-week median and MAD, with region and global fallback for new keys) runs beside the static detector, pages through Alertmanager, shows its baseline band in Grafana, and `make demo` warms it on one continuous 3-week timeline.

**Architecture:** `MinuteAggregator` output now feeds two operators. `PopulationBaselines` (parallelism 1) keeps per-region and global histograms per hour-of-week bucket and broadcasts next week's fallback baseline when each hour ends. `Scorer` becomes a `KeyedBroadcastProcessFunction`: it keeps each key's weekly ring of minute values, scores every minute against the key's own baseline or the broadcast fallback, runs the seasonal settings and the static sets through the same state machine, writes scores to a side output and expires idle keys. The Python generator backfills 3 weeks in whole-minute batches, then continues second by second.

**Tech stack:** unchanged from milestone 1 (Java 17, Flink 2.2.1, flink-connector-kafka 5.0.0-2.2, JUnit 5.11.4, AssertJ 3.27.7; Python 3.12 with uv, confluent-kafka 2.16.1, numpy 2.5.4, orjson 3.13.0; ClickHouse 26.8.36.3, Grafana 13.2.5). No new dependencies.

**Spec:** `docs/superpowers/specs/2026-11-09-tenant-baselines-design.md` (v2, approved). Milestone 2 is §8 item 2. Read §4.2–§4.5, §4.9, §4.10, §7 and §10 before starting. Follow-ups folded in come from `docs/superpowers/m1-followups.md`.

## Global Constraints

- Everything in the milestone 1 plan's Global Constraints still applies (`docs/superpowers/plans/2026-11-09-m1-static-end-to-end.md`), with one correction: the TaskManager runs at **2560m** (`taskmanager.memory.process.size` and `mem_limit`), as `docker-compose.yml` has it, not the 3072m that plan wrote.
- Java package root `io.github.sanchaypahalwani.tenantbaselines` (written `<pkg>`). Tests run with `make test-java`; Python with `make test-py && make lint-py`.
- **Buckets:** 168 hour-of-week buckets in UTC. A bucket keeps one-minute values in a ring of **5 weekly slots × 60 minutes**. Baseline = **median** and `1.4826 × MAD` over the **last 4 weeks, not counting the current week**.
- **Warm:** a key is warm for a signal in a bucket with **≥ 2 weeks** of data there; a week counts with **≥ 30** stored minutes.
- **Scores** (distance from baseline in spreads, bad direction only):
  - error rate: spread `max(1.4826·MAD, √(p₀(1−p₀)/n))`, `p₀ = max(median, 0.005)`, `n` the minute's count;
  - latency p95: spread `max(1.4826·MAD, 0.05 × median)`;
  - volume: trailing 5-minute sum, expected = sum of the 5 minutes' bucket medians, spread `max(√5 · 1.4826·MAD, √expected)`.
- **Minimum evidence** 20 requests for error rate and latency; an unscored minute pauses both streaks. Fire after 3, resolve after 5 (unchanged).
- **Seasonal default:** fire at score ≥ 4, resolve at score < 2. Detector name `seasonal`, setting name `k4`.
- **Zero-traffic rule:** 5-minute sum of 0 with ≥ 15 expected counts as a breach, for every key and every detector.
- **Cold start:** error rate and latency fall back to the region's baseline, then the global one. Volume never falls back.
- **Key expiry:** 7 days of event time without a request; firing alerts get a `resolved` event with reason `expired`.
- **Output JSON never contains `NaN` or `Infinity`.** A value that doesn't exist is `null`.
- **State:** every stateful operator has a `uid`. Existing state names (`volume-ring`, `machines`, `open-minutes`, `last-event-ms`) and POJO class names stay; POJOs only gain fields. Every state class must be a Flink POJO (public class, public no-arg constructor, public non-final fields), never a Kryo fallback.
- **Static thresholds:** `infra/flink/thresholds-demo.json` is not changed in this milestone (the thresholds script is milestone 4).
- **Kafka record timestamps** stay at produce time. Never set them to event time: `live.events` has `retention.ms=86400000` and would delete the history.
- **Repo rules** (unchanged): no employer or company names, no marketing vocabulary, synthetic data labelled synthetic, commit locally after each task, never push.

## Open questions for the owner

Both questions are resolved; the owner's answers are recorded below.

1. **Demo warm-up path. Resolved:** the owner chose A on 2026-11-10. Three weeks of the demo world is about 20.7 million events (computed from `build_world("demo", 1)` and `traffic.py`). Measured on the host: the current per-second `tick` needs about 105 s of pure generation for 3 weeks (1.8 million ticks), before any Kafka produce; a per-minute numpy batch needs about 12 s. Produce is estimated at 50–70 s either way, and Flink catch-up at 100–200 s on parallelism 2 (an estimate: one RocksDB read-modify-write of an 830-byte accumulator per event; not measured).
   - **A (recommended, planned in Task 9):** backfill in whole-minute batches, live second by second. Estimated warm-up 2–3.5 minutes, bound by Flink. Cost: a second generation method and its tests; the two methods use different random draws.
   - **B:** keep per-second ticks for history. No new path. Estimated 3–4.5 minutes, bound by the generator.
   - **C:** halve the demo traffic. Halves both sides, but puts more keys under minimum evidence and departs from §5.2's median of 150.
   - Whichever is chosen, Task 10 measures the real figure and prints it in the demo hint. If it exceeds 4 minutes, the task stops and reports instead of tuning.
2. **Spec wording, forwarder stale rule (§4.10). Resolved:** the owner approved the rule as built on 2026-11-10, and the spec was amended in commit 05d2e8c. No task changes it.

## Review Focus

1. **A job that starts before any event exists, or an events partition that stays empty.** The live watermark latches an empty split to the wall clock after 60 s; three-week-old events would then all be dropped as late. Expected: history is aggregated. Pinned in Task 9 (`test_partition_of_covers_all_partitions`) and Task 10 (`demo.sh` submits only after every partition has data; smoke check 1 requires aggregates at least 20 days old).
2. **Unscored minutes and expired alerts in the JSON output.** A `NaN` would land in `tb.ingest_errors` or break ClickHouse parsing. Expected: `null` fields, and no ingest errors. Pinned in Task 6 (`scoresAreEmittedPerSignalWithBand` asserts no non-finite number), Task 8 (`scoreEventWritesNulls`) and Task 10 (smoke check 7).
3. **A watermark that jumps more than a week** (restore after long downtime, or all three weeks read before the first watermark). The fallback broadcast then arrives after the minutes that needed it. Expected: those minutes are unscored; a baseline for a later week is never used early; nothing fails. Pinned in Task 7 (`fallbackForNextWeekIsNotUsedEarly`, `coldKeyIsUnscoredWithoutFallback`).
4. **The job restarts during or after warm-up.** Expected: each key's history, the hour cache and the fallback baselines come back from the checkpoint, and state from a milestone 1 checkpoint still loads. Pinned in Task 7 (`baselinesSurviveSnapshotAndRestore`, `stateTypesArePojos`) and Task 4 (`toleratesMissingNewFields`).
5. **`make demo` run a second time on a stack that already holds data.** The second generator's history is older than the job's watermark and state. Expected: a refusal that names `make reset`, not a silent half-working demo. Pinned in Task 10 (`demo.sh` guard, checked by hand in Step 3).

## File map

```
flink-job/src/main/java/<pkg>/
  baseline/HourOfWeek.java  RobustStats.java  Scores.java  Baseline.java          (new, Task 2)
  baseline/BucketHistory.java  HourCache.java                                     (new, Task 3)
  detect/VolumeRing.java                                                          (modify, Task 4)
  detect/ZeroTraffic.java                                                         (new, Task 4)
  baseline/ErrorRateBins.java  HistogramStats.java  PopHistogram.java
  baseline/PopulationBaseline.java  PopulationBaselines.java                      (new, Task 5)
  detect/SeasonalSetting.java  model/ScoreEvent.java                              (new, Task 6)
  detect/Scorer.java                                                              (modify, Tasks 6 and 7)
  JobConfig.java  Pipeline.java  TenantBaselinesJob.java  io/JsonSerialization.java  (modify, Task 8)
flink-job/src/test/java/<pkg>/...        one test class per class above; LevelTest and JobConfigTest are new
infra/clickhouse/init/01-schema.sql  infra/grafana/dashboards/tenant-baselines.json  docker-compose.yml  (Task 8)
py/src/tenant_baselines/{incidents,traffic,gen_live,forwarder}.py  py/tests/...   (Tasks 1, 8, 9)
scripts/demo.sh (new)  scripts/smoke.sh  Makefile  README.md  docs/superpowers/m1-followups.md  (Task 10)
```

---

### Task 1: Incident multiplier and generator tidy-up

Follow-ups from milestone 1 that the later tasks build on.

**Files:**
- Modify: `py/src/tenant_baselines/incidents.py`, `traffic.py`, `gen_live.py`; `py/tests/test_incidents.py`, `test_traffic.py`, `test_gen_live.py`

**Interfaces (produces):**
- `Incident.kind: Literal["error_spike", "volume_mult", "latency_spike"]`. For `volume_mult`, `magnitude` is the factor traffic is multiplied by (0.3 means 30% remains; 3.0 is a surge). `__post_init__` raises `ValueError` for a negative `volume_mult` magnitude. `effects()` multiplies `volume_mult` by it. The kind `volume_drop` no longer exists.
- `traffic.key_rate_rpm(tenant: Tenant, n_keys: int, t_ms: int) -> float` returns `tenant.peak_rpm * load(tenant, t_ms) / n_keys`. `LiveGenerator.key_rate_rpm` and `LiveGenerator.tick` both call it (the formula was written twice).
- `traffic.error_prob(load_frac: float)` and `traffic.latency_median_ms(base: float, load_frac: float)`: the parameter is renamed so it no longer shadows `load()`.

- [ ] **Step 1: Write the failing tests**
  - `test_incidents.py::test_volume_mult_scales_volume`: `Incident("volume_mult", 0, 60_000, 0.3, key=k.key)` gives `effects(...).volume_mult == 0.3` inside the window and `1.0` outside; two overlapping incidents of 0.5 give 0.25.
  - `test_incidents.py::test_negative_volume_mult_is_rejected`: `Incident("volume_mult", 0, 1, -0.1)` raises `ValueError`.
  - `test_traffic.py::test_key_rate_splits_tenant_rate`: for a tenant with `peak_rpm=120`, at a weekday local noon, `key_rate_rpm(t, 3, ms) == 40.0`.
  - Update existing tests that use `"volume_drop"`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-py` → the three new tests fail.
- [ ] **Step 3: Implement the three changes above.**
- [ ] **Step 4: Run tests and lint.** `make test-py && make lint-py` → all pass. `git grep -n volume_drop -- py` → no matches.
- [ ] **Step 5: Commit.** `git add py && git commit -m "Make incident volume a multiplier and share the key rate formula"`

### Task 2: Hour-of-week buckets, robust statistics and score floors

**Files:**
- Create: `baseline/HourOfWeek.java`, `baseline/RobustStats.java`, `baseline/Baseline.java`, `baseline/Scores.java`
- Test: `baseline/HourOfWeekTest.java`, `baseline/RobustStatsTest.java`, `baseline/ScoresTest.java`

**Interfaces:**
- Consumes: `model.Signal` (`ERROR_RATE`, `LATENCY_P95`, `VOLUME_5M`; `direction()`), `detect.Judgement`.
- Produces:
  - `final class HourOfWeek`: constants `HOUR_MS = 3_600_000L`, `WEEK_MS = 604_800_000L`. Weeks start Monday 00:00 UTC (1970-01-05 is the first Monday, so the shift is 3 days).
    - `static long week(long ms)` = `Math.floorDiv(ms + 259_200_000L, WEEK_MS)`
    - `static int bucket(long ms)` = `(int) (Math.floorMod(ms + 259_200_000L, WEEK_MS) / HOUR_MS)`, 0–167
    - `static int minuteOfHour(long ms)` = `(int) (Math.floorMod(ms, HOUR_MS) / 60_000L)`
    - `static long hourStart(long ms)` = `ms − Math.floorMod(ms, HOUR_MS)`
  - `final class RobustStats`: `MAD_SCALE = 1.4826`.
    - `static double median(double[] v, int n)`: sorts the first `n` entries in place; for even `n` the mean of the two middle values; `NaN` for `n == 0`.
    - `static double mad(double[] v, int n, double median)`: the median of `|v[i] − median|`.
  - `record Baseline(double median, double scaledMad) implements java.io.Serializable`; `scaledMad` is already `1.4826 × MAD`.
  - `final class Scores`:
    - `static double errorRateSpread(double median, double scaledMad, long n)`
    - `static double latencySpread(double median, double scaledMad)`
    - `static double volumeSpread(double scaledMadOfSum, double expected)` = `max(scaledMadOfSum, √expected)`
    - `static double score(Signal s, double value, double reference, double spread)`: `(value − reference) / spread` for UP, `(reference − value) / spread` for DOWN; `NaN` when any input is `NaN` or `spread <= 0`.
    - `static Judgement judge(double score, double fireK, double resolveK)`: `NaN` → `UNSCORED`; `score >= fireK` → `BREACH`; `score < resolveK` → `CALM`; else `NEITHER`.
    - `static double fireReference(Signal s, double reference, double spread, double fireK)`: `reference + fireK·spread` for UP, `reference − fireK·spread` for DOWN.

- [ ] **Step 1: Write the failing tests**
  - `HourOfWeekTest.mondayMidnightIsBucketZero`: `bucket(345_600_000L) == 0`, `week(345_600_000L) == 1`, `bucket(345_599_999L) == 167`, `week(345_599_999L) == 0`.
  - `HourOfWeekTest.epochIsThursday`: `bucket(0) == 72`; `bucket(-1) == 71`; `minuteOfHour(3_660_000L) == 1`; `hourStart(3_660_000L) == 3_600_000L`.
  - `RobustStatsTest.medianOddAndEven`: `{5,1,3}` → 3; `{4,1,3,2}` → 2.5; `n = 0` → `NaN`.
  - `RobustStatsTest.madIgnoresOneOutlier`: values `{1,2,3,4,100}` have median 3 and MAD 1.
  - `ScoresTest.errorRateSamplingFloor`: `errorRateSpread(0, 0, 37)` ≈ 0.011596 (±1e-6); `score(ERROR_RATE, 1.0/37, 0, that)` ≈ 2.33 and with `2.0/37` ≈ 4.66 (±0.01), the spec's worked example.
  - `ScoresTest.errorRateUsesMedianAboveHalfPercent`: `errorRateSpread(0.02, 0.001, 100)` ≈ 0.014; `errorRateSpread(0.02, 0.05, 100) == 0.05`.
  - `ScoresTest.latencyFivePercentFloor`: `latencySpread(100, 2) == 5`; `latencySpread(100, 8) == 8`.
  - `ScoresTest.volumeSquareRootFloor`: `volumeSpread(3, 100) == 10`; `volumeSpread(20, 100) == 20`; `score(VOLUME_5M, 60, 100, 10) == 4`.
  - `ScoresTest.zeroSpreadIsUnscored`: `score(LATENCY_P95, 5, 0, 0)` is `NaN`.
  - `ScoresTest.judgeBoundaries`: with `(4, 2)`: 4.0 → `BREACH`, 3.999 → `NEITHER`, 2.0 → `NEITHER`, 1.999 → `CALM`, −3 → `CALM`, `NaN` → `UNSCORED`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java` → compilation failure.
- [ ] **Step 3: Implement the four classes.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures.
- [ ] **Step 5: Commit.** `git add flink-job && git commit -m "Add hour-of-week buckets, robust statistics and score floors"`

### Task 3: Weekly ring per bucket and the hour cache

**Files:**
- Create: `baseline/BucketHistory.java`, `baseline/HourCache.java`
- Test: `baseline/BucketHistoryTest.java`, `baseline/HourCacheTest.java`

**Interfaces:**
- Consumes: `RobustStats`, `Baseline`, `Signal`.
- Produces:
  - `class BucketHistory` (Flink POJO), one per key and bucket:
    - constants `SLOTS = 5`, `MINUTES = 60`, `BASELINE_WEEKS = 4`, `MIN_MINUTES_PER_WEEK = 30`, `MIN_WEEKS = 2`;
    - `public long[] slotWeek` (5 entries, each `Long.MIN_VALUE` when new);
    - `public float[] values` (3 × 5 × 60 = 900 entries, `NaN` when new; index `(s.ordinal() * SLOTS + slot) * MINUTES + minuteOfHour`);
    - `void put(long week, int minuteOfHour, Signal s, double value)`;
    - `int weeksWithData(Signal s, long currentWeek)`;
    - `Baseline baseline(Signal s, long currentWeek)`, `null` when the window holds no value.
  - `class HourCache` (Flink POJO): `public long hourStartMs = Long.MIN_VALUE; public double[] median = new double[3]; public double[] scaledMad = new double[3]; public boolean[] warm = new boolean[3]`, indexed by `Signal.ordinal()`.
    - `static HourCache of(BucketHistory h, long hourStartMs)`; `h` may be `null` (nothing warm).
    - `Baseline own(Signal s)` returns the baseline when `warm[s]`, else `null`.

**The ring** (what the signatures don't settle):

```
put(week, minute, s, value):
    slot = floorMod(week, SLOTS)
    if slotWeek[slot] != week:            # the slot held a week from 5+ weeks ago
        set all 3 × 60 values of that slot to NaN
        slotWeek[slot] = week
    values[index(s, slot, minute)] = (float) value

window(currentWeek) = slots whose slotWeek is in [currentWeek − 4, currentWeek − 1]
weeksWithData(s, currentWeek) = number of window slots with ≥ 30 non-NaN values for s
baseline(s, currentWeek)      = median and 1.4826·MAD over every non-NaN value for s in the window
HourCache.of: week = HourOfWeek.week(hourStartMs); warm[s] = weeksWithData(s, week) ≥ 2
```

The current week's slot is never in the window, so a bucket's baseline cannot change during its hour; that is why the Scorer computes it once per hour.

- [ ] **Step 1: Write the failing tests**
  - `BucketHistoryTest.baselineIsMedianAndScaledMadOfPastWeeks`: weeks 10 and 11 each hold 60 `LATENCY_P95` values, `100 + (minute % 3)` → `baseline(LATENCY_P95, 12)` has median 101 and `scaledMad == 1.4826`; `weeksWithData == 2`.
  - `currentWeekIsExcluded`: the same history plus 60 values of 900 in week 12 → `baseline(LATENCY_P95, 12).median() == 101`.
  - `onlyLastFourWeeksCount`: values 1, 2, 3, 4, 5 (60 each) in weeks 10–14 → `baseline(s, 15).median() == 3.5` (weeks 11–14) and `weeksWithData(s, 15) == 4`.
  - `weekNeedsThirtyMinutes`: 29 values in week 10 and 60 in week 11 → `weeksWithData(s, 12) == 1`.
  - `slotIsWipedWhenReused`: 60 values in week 10, then one value in week 15 (same slot) → `baseline(s, 16)` has median equal to that one value.
  - `staleSlotsAreIgnored`: 60 values in each of weeks 10 and 11 → `baseline(s, 30) == null` and `weeksWithData(s, 30) == 0`.
  - `signalsAreIndependent`: values for `ERROR_RATE` only → `baseline(VOLUME_5M, …) == null`.
  - `HourCacheTest.warmNeedsTwoWeeks`: built from a history with weeks 10–11 → `warm` true and `own(s)` non-null at an hour in week 12; `HourCache.of(null, hour)` has nothing warm.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java`
- [ ] **Step 3: Implement both classes.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures.
- [ ] **Step 5: Commit.** `git add flink-job && git commit -m "Add the weekly ring of minute values per hour-of-week bucket"`

### Task 4: Volume expectation and the zero-traffic rule

**Files:**
- Modify: `detect/VolumeRing.java`
- Create: `detect/ZeroTraffic.java`
- Test: `detect/VolumeRingTest.java` (new; move the ring test out of `MinuteSignalsTest`), `detect/ZeroTrafficTest.java`

**Interfaces:**
- Consumes: `VolumeRing` as built (`long lastMinuteMs; long[] counts; int filled; double push(long minuteMs, long count)`), `Scores.volumeSpread`, `Judgement`.
- Produces:
  - `VolumeRing` gains fields `public double[] medians; public double[] scaledMads; public long[] before; public int beforeFilled;` (5, 5 and 30 entries) and methods:
    - `double push(long minuteMs, long count, double median, double scaledMad)`: as `push` today, and also shifts the minute's bucket median and scaled MAD into the two new 5-rings (`NaN` when the key is cold there) and the count leaving the 5-minute window into `before`. A gap resets everything.
    - `double push(long minuteMs, long count)` stays and delegates with `NaN, NaN`.
    - `double expected()`: the sum of `medians`, `NaN` until 5 minutes are held or if any is `NaN`.
    - `double spread()`: `Scores.volumeSpread(√(Σ scaledMads[i]²), expected())`, `NaN` when `expected()` is. Within one bucket this equals the spec's `√5 · 1.4826·MAD`.
    - `double trailingExpected()`: `5 × mean(before)`, the average of the 30 minutes before the current 5-minute window; `NaN` until 30 are held.
  - `final class ZeroTraffic`: `MIN_EXPECTED = 15.0`; `static Judgement adjust(Judgement own, double volume5m, double expected5m)`.

**`adjust`:**

```
if volume5m is NaN:                          return own        # window not full yet
if volume5m == 0 and expected5m >= 15:       return BREACH
if own == UNSCORED and expected5m is not NaN: return volume5m > 0 ? CALM : NEITHER
return own
```

The third line lets an alert raised by the rule on a cold key resolve once traffic returns. The caller passes `ring.expected()` when it is not `NaN`, otherwise `ring.trailingExpected()`.

Fields added to a POJO come back as `null` from state written before they existed, so `push` allocates any of the four new arrays that is `null`.

- [ ] **Step 1: Write the failing tests**
  - `VolumeRingTest.expectedIsSumOfFiveMedians`: five pushes with medians 10, 20, 30, 40, 50 and scaled MADs 3, 3, 3, 3, 3 → `expected() == 150`, `spread()` ≈ `max(√45, √150)` = 12.247.
  - `madTermWinsWhenLarger`: medians 4 each, scaled MADs 5 each → `spread()` ≈ `√5 · 5` = 11.180.
  - `expectedIsNaNWhileAnyMinuteIsCold`: one of the five pushes has `NaN` median → `expected()` and `spread()` are `NaN`.
  - `trailingExpectedUsesThirtyMinutesBeforeTheWindow`: 30 pushes of count 4 then 5 pushes of count 0 → `trailingExpected() == 20`; after only 34 pushes it is `NaN`.
  - `gapResetsEverything`: after a skipped minute, `expected()` and `trailingExpected()` are `NaN`.
  - `toleratesMissingNewFields`: a ring with the four new arrays set to `null` accepts a push and returns the same sum as before.
  - `ZeroTrafficTest`: `adjust(CALM, 0, 15) == BREACH`; `adjust(CALM, 0, 14.9) == CALM`; `adjust(UNSCORED, 0, 20) == BREACH`; `adjust(UNSCORED, 7, 20) == CALM`; `adjust(UNSCORED, 0, 10) == NEITHER`; `adjust(UNSCORED, 0, NaN) == UNSCORED`; `adjust(NEITHER, NaN, 100) == NEITHER`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java`
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures, including the unchanged `ScorerTest`.
- [ ] **Step 5: Commit.** `git add flink-job && git commit -m "Add the volume expectation and the zero-traffic rule"`

### Task 5: PopulationBaselines

**Files:**
- Create: `baseline/ErrorRateBins.java`, `baseline/HistogramStats.java`, `baseline/PopHistogram.java`, `baseline/PopulationBaseline.java`, `baseline/PopulationBaselines.java`
- Test: `baseline/ErrorRateBinsTest.java`, `baseline/HistogramStatsTest.java`, `baseline/PopulationBaselinesTest.java`

**Interfaces:**
- Consumes: `MinuteAgg(runId, key, tenant, region, unit, minuteMs, count, errors, p95LatencyMs)`, `agg.LatencyHistogram` (`BUCKETS = 202`, `bucketOf`, `representative`), `MinuteSignals.MIN_REQUESTS`, `HourOfWeek`, `RobustStats.MAD_SCALE`.
- Produces:
  - `final class ErrorRateBins`: `BINS = 160`. `static int bucketOf(double rate)`: 0 for `rate < 1e-4`, else `1 + min(158, (int) floor(ln(rate / 1e-4) / ln(1.06)))`. `static double representative(int bin)`: 0 for bin 0, else `min(1.0, 1e-4 · 1.06^(bin−1) · √1.06)`.
  - `final class HistogramStats`: `static double median(int[] counts, IntToDoubleFunction representative)` and `static double mad(int[] counts, IntToDoubleFunction representative, double median)`; both `NaN` for an empty histogram.
  - `class PopHistogram` (Flink POJO): `public int[] errorRate = new int[160]; public int[] latency = new int[202];` with `void add(double errorRate, double p95)`, `void addAll(PopHistogram other)`, `long n()` (the sum of `latency`).
  - `record PopulationBaseline(String scope, int bucket, long week, double errorMedian, double errorScaledMad, double latencyMedian, double latencyScaledMad, long n)`, with `static final String GLOBAL = "*"` and `Baseline of(Signal s)` (`null` for `VOLUME_5M`). `scope` is a region name or `GLOBAL`. `week` is the week the baseline is for.
  - `class PopulationBaselines extends KeyedProcessFunction<String, MinuteAgg, PopulationBaseline>`: no-arg constructor; `static final String SINGLE_KEY = "all"`; `static final long MIN_VALUES = 30`. The caller keys the stream by the constant `SINGLE_KEY` and sets parallelism 1; a keyed function is used because only keyed functions have timers.

**Median and MAD from a histogram:**

```
median(counts, rep): n = Σ counts; rank = ceil(n / 2); walk bins upward to the first bin
                     where the running count ≥ rank; return rep(bin)
mad(counts, rep, m): for each non-empty bin make (|rep(bin) − m|, counts[bin]); sort by the
                     first; walk to the first pair where the running count ≥ ceil(n / 2);
                     return its deviation            # the caller multiplies by 1.4826
```

**Operator behaviour:**
- **State:** `MapState<String, PopHistogram>` named `pop-histograms`, keyed `scope + "|" + bucket + "|" + week`; `MapState<String, Boolean>` named `pop-scopes`.
- **`processElement`:** skip minutes with `count < 20` or a null p95. Otherwise add `errors/count` and the p95 to the entries of the minute's region and of `GLOBAL`, record both scopes, and register an event-time timer at `HourOfWeek.hourStart(minuteMs) + HOUR_MS − 1`.
- **`onTimer(t)`:** the hour that ended has bucket `b` and week `w`. For each recorded scope, merge the entries of weeks `w−3 … w`. If the merged `n() ≥ 30`, emit `PopulationBaseline(scope, b, w + 1, …)`. Remove the entry of week `w − 4`.
- The timer fires once every upstream subtask's watermark has passed the hour, so all of the hour's minutes are in. Timers are registered only from elements, so the final `Long.MAX_VALUE` watermark publishes the last hour and stops.

**Test harness:** `ProcessFunctionTestHarnesses.forKeyedProcessFunction(new PopulationBaselines(), a -> PopulationBaselines.SINGLE_KEY, Types.STRING)`. Use `H = 345_600_000L` (week 1, bucket 0).

- [ ] **Step 1: Write the failing tests**
  - `ErrorRateBinsTest.zeroHasItsOwnBin`: `bucketOf(0) == 0`, `representative(0) == 0`, `bucketOf(1e-4) == 1`, `bucketOf(1.0) == 159`.
  - `ErrorRateBinsTest.representativeWithinThreePercent`: for rates 0.001, 0.01, 0.2 and 0.9, `|representative(bucketOf(r)) − r| / r ≤ 0.03`.
  - `HistogramStatsTest.medianAndMad`: latency counts built from 40 × 100 ms, 30 × 90 ms, 30 × 110 ms → median equals `representative(bucketOf(100))`; MAD equals the smaller of `|representative(bucketOf(90)) − median|` and `|representative(bucketOf(110)) − median|`. An empty histogram gives `NaN` for both.
  - `PopulationBaselinesTest.publishesRegionAndGlobalAtHourEnd`: keys `a|eu|u1` and `b|eu|u1` and `c|us|u2`, each with 60 minutes from `H`, count 100, errors 1, p95 100. `processWatermark(H + 3_599_998)` → no output. `processWatermark(H + 3_599_999)` → exactly 3 records, scopes `eu`, `us` and `*`, each with `bucket 0`, `week 2`; `n` is 120, 60 and 180; `errorMedian` within 3% of 0.01; `latencyMedian` within 3% of 100; both scaled MADs 0.
  - `skipsMinutesBelowMinimumEvidence`: 60 minutes with count 19 → no output at the hour end.
  - `needsThirtyValues`: one key with 29 scored minutes → no output; with 30 → output.
  - `usesLastFourWeeks`: one key, the same bucket in weeks 1–5 with p95 100, 200, 300, 400, 500 (60 minutes each, watermark advanced past each hour) → the record published at the end of week 5 has `week 6`, `n == 240` and `latencyMedian` within 3% of 300.
  - `finalWatermarkPublishesOnce` (`@Timeout(2)`): 60 minutes, then `processWatermark(Long.MAX_VALUE)` → the records for that hour and nothing more.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java`
- [ ] **Step 3: Implement the five classes.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures.
- [ ] **Step 5: Commit.** `git add flink-job && git commit -m "Add PopulationBaselines with histogram-based fallback baselines"`

### Task 6: Seasonal detector in the Scorer

The Scorer changes base class here, so later tasks only add behaviour. This task scores against the key's own baseline; Task 7 adds fallback and expiry.

**Files:**
- Create: `detect/SeasonalSetting.java`, `model/ScoreEvent.java`
- Modify: `detect/Scorer.java`, `Pipeline.java` (the topology below, with fixed arguments until Task 8)
- Test: `detect/ScorerTest.java` (existing tests move to the new harness), `detect/SeasonalScorerTest.java`, `detect/LevelTest.java`; add to `detect/MinuteSignalsTest.java`, `detect/AlertStateMachineTest.java`

**Interfaces:**
- Consumes: Tasks 2–5; `ThresholdSet`, `Level.judge`, `AlertStateMachine.step`, `AlertIds.of(detector, setting, scope, subject, signal, startsAtMs)`, `MinuteSignals.of(MinuteAgg, double volume5m)`, `AlertEvent` (17 fields, unchanged).
- Produces:
  - `record SeasonalSetting(String setting, double fireK, double resolveK) implements java.io.Serializable`; `static SeasonalSetting ofK(double k)` gives name `"k4"` for 4.0 (`"k4.5"` for 4.5) and `resolveK = k / 2`.
  - `record ScoreEvent(String runId, String key, String tenant, String region, String unit, String signal, long minuteMs, Double value, Double median, Double spread, Double score, String source)`. `source` is `"own"`, `"region"`, `"global"` or `null`. Every `Double` is `null` when it doesn't exist; none is ever `NaN` or infinite.
  - `class Scorer extends KeyedBroadcastProcessFunction<String, MinuteAgg, PopulationBaseline, AlertEvent>`:
    - constructor `(String runId, List<ThresholdSet> sets, List<SeasonalSetting> seasonal, boolean emitScores, Duration expiry)`;
    - `public static final OutputTag<ScoreEvent> SCORES = new OutputTag<>("scores") {};`
    - `public static final MapStateDescriptor<String, PopulationBaseline> POPULATION = new MapStateDescriptor<>("population-baselines", Types.STRING, TypeInformation.of(PopulationBaseline.class));`
    - `public static final String SEASONAL = "seasonal";`
  - `Pipeline.build` (signature and `Outputs` unchanged in this task) becomes:
    ```
    aggregates  = events.keyBy(Event::key).process(MinuteAggregator)                     uid minute-aggregator
    population  = aggregates.keyBy(a -> PopulationBaselines.SINGLE_KEY, Types.STRING)
                      .process(new PopulationBaselines()).setParallelism(1)              uid population-baselines
    alerts      = aggregates.keyBy(MinuteAgg::key)
                      .connect(population.broadcast(Scorer.POPULATION))
                      .process(new Scorer(cfg.runId(), sets, List.of(SeasonalSetting.ofK(4)), false, Duration.ofDays(7)))
                                                                                         uid scorer
    ```

**State.** Keep `volume-ring` (`ValueState<VolumeRing>`) and `machines` (`MapState<String, AlertStateMachine>`, key `detector/setting/signal`). Add `bucket-history` (`MapState<Integer, BucketHistory>`, keyed by bucket) and `hour-cache` (`ValueState<HourCache>`).

**Per minute** (`processElement`; `m = a.minuteMs()`):

```
1. cache = hour-cache; if null or cache.hourStartMs != hourStart(m):
       cache = HourCache.of(bucket-history.get(bucket(m)), hourStart(m)); store it
2. vol = cache.own(VOLUME_5M)                       # null while the key is cold here
   volume = ring.push(m, a.count(), vol median or NaN, vol scaledMad or NaN)
   signals = MinuteSignals.of(a, volume)
3. reference per signal:
     ERROR_RATE:  b = cache.own(s)  [Task 7: else region, else global]
                  spread = Scores.errorRateSpread(b.median, b.scaledMad, a.count())
     LATENCY_P95: likewise with Scores.latencySpread
     VOLUME_5M:   reference = ring.expected(), spread = ring.spread()   (own baseline only)
     score = Scores.score(s, signals.get(s), reference, spread); NaN when there is no baseline
4. zeroExpected = ring.expected() if not NaN else ring.trailingExpected()
5. for each SeasonalSetting, for each Signal:
       j = Scores.judge(score, fireK, resolveK)
       if s == VOLUME_5M: j = ZeroTraffic.adjust(j, volume, zeroExpected)
       step the machine "seasonal/<setting>/<signal>"; on FIRE or RESOLVE emit an AlertEvent:
           detector "seasonal", value = signals.get(s),
           reference = Scores.fireReference(s, reference, spread, fireK), or 0.0 when there is no baseline,
           reason = "zero_traffic" when this is a FIRE and adjust changed the judgement, else null
   for each active ThresholdSet: as today, with the same ZeroTraffic.adjust on VOLUME_5M
       (and the same "zero_traffic" reason)
6. feed the baseline: h = bucket-history.get(bucket(m)) or new; with week(m), minuteOfHour(m):
       h.put(ERROR_RATE, signals.errorRate()) and h.put(LATENCY_P95, signals.latencyP95()) when not NaN;
       h.put(VOLUME_5M, a.count()) always; store h
7. if emitScores: ctx.output(SCORES, …) one ScoreEvent per signal with value, median (= reference),
       spread, score, source ("own" when the key's baseline was used); NaN becomes null
```

Every minute feeds the baseline, including minutes while an alert is firing (step 6 has no condition on alert state). `processBroadcastElement` is empty until Task 7.

**Test harness:** `ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(scorer, MinuteAgg::key, Types.STRING, Scorer.POPULATION)` returns a `KeyedBroadcastOperatorTestHarness` (verified in flink-runtime 2.2.1's test jar). It has `processElement(value, ts)`, `processBroadcastElement(value, ts)`, `watermark(long)` (both inputs), `extractOutputValues()` and `getSideOutput(tag)`.

Seasonal tests use `sets = List.of()`, `List.of(SeasonalSetting.ofK(4))`, key `a|eu|u1`, `MON = 345_600_000L`, `WEEK = 604_800_000L`, and a helper `history(h, fromMs, toMs, count, errors, p95)` that feeds one `MinuteAgg` per minute. "Two weeks of history" means `history(h, MON, MON + 2·WEEK, 100, 0, 100.0)`; `W3 = MON + 2·WEEK`.

- [ ] **Step 1: Write the failing tests**
  - `ScorerTest`: the three existing tests pass unchanged in meaning with the new constructor and harness (`emitScores = false`, `seasonal = List.of()`).
  - `SeasonalScorerTest.errorRateFiresAgainstOwnBaseline`: two weeks of history, then minutes `W3 … W3 + 2 min` with count 100, errors 10 → exactly one event: `detector "seasonal"`, `setting "k4"`, `signal "error_rate"`, `state "firing"`, `startsAtMs == W3`, `eventTimeMs == W3 + 180_000`, `value == 0.1`, `reference` ≈ 0.028214 (±1e-5), `alertId == AlertIds.of("seasonal", "k4", "tenant", key, "error_rate", W3)`.
  - `oneWeekOfHistoryIsNotEnough`: one week of history, then the same spike at `MON + WEEK` → no events.
  - `weekNeedsThirtyScoredMinutes`: history where bucket 0 has count 100 for only its first 29 minutes each week (count 5 otherwise), then the spike at `W3` → no events.
  - `latencyUsesFivePercentFloor`: two weeks of history; three minutes at `W3` with p95 120.0 → one `latency_p95` firing event with `reference == 120.0`. A fresh run with p95 119.9 → no events.
  - `volumeDropFiresOnFiveMinuteSum`: two weeks of history; from `W3`, count 80 per minute for 8 minutes → one `volume_5m` firing event with `startsAtMs == W3 + 240_000`, `eventTimeMs == W3 + 420_000`, `value == 400`, `reference` ≈ 410.557 (±0.01), `reason == null`.
  - `coldVolumeIsUnscored`: one week of history, then count 30 for 20 minutes → no events.
  - `zeroTrafficFiresOnColdKey`: from `MON`, 40 minutes of count 4, then 8 minutes of count 0 → one `volume_5m` firing event with `startsAtMs == MON + 44 min`, `reason == "zero_traffic"`, `value == 0`, `reference == 0.0`.
  - `zeroTrafficNeedsFifteenExpected`: 40 minutes of count 2, then 8 of count 0 → no events.
  - `zeroTrafficAlertResolvesWhenTrafficReturns`: continue the firing case with 5 minutes of count 4 → one `resolved` event with the same `alertId`.
  - `currentWeekDoesNotMoveTheBaseline`: two weeks of history, then 40 minutes at `W3` with errors 50; with `emitScores = true`, the `error_rate` score of the 40th minute equals that of the 1st (±1e-9).
  - `scoresAreEmittedPerSignalWithBand`: `emitScores = true`. Each minute adds exactly 3 `ScoreEvent`s. At a warm minute the `error_rate` event has `median == 0.0`, `spread` ≈ 0.0070534, `source == "own"`. At `MON` (cold) `median`, `spread`, `score` and `source` are `null` and `value` is set. No `Double` in any `ScoreEvent` or `AlertEvent` of the run is `NaN` or infinite.
  - `noScoresWhenDisabled`: `emitScores = false` → the side output is empty.
  - `staticAndSeasonalRunSideBySide`: demo thresholds and `k4`; two weeks of history, then 3 minutes with errors 10 → two firing `error_rate` events, one per detector, with different `alertId`s.
  - Follow-up tests: `MinuteSignalsTest.exactlyTwentyRequestsIsScored` (count 20 scored, 19 not); `AlertStateMachineTest.unscoredPausesCalmStreak` (firing, C, C, U, C, C, C → `RESOLVE` on the last C); `LevelTest.boundaries` (`Level(0.05, 0.025)` UP: 0.05 → `BREACH`, 0.0499 → `NEITHER`, 0.025 → `NEITHER`, 0.0249 → `CALM`; `Level(0, 5)` DOWN: 0 → `BREACH`, 5 → `NEITHER`, 5.1 → `CALM`; `NaN` → `UNSCORED`).
- [ ] **Step 2: Run them and confirm they fail.** `make test-java`
- [ ] **Step 3: Implement as specified above.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures, including `PipelineTest` and `TenantBaselinesJobTest`.
- [ ] **Step 5: Commit.** `git add flink-job && git commit -m "Score each minute against the key's hour-of-week baseline"`

### Task 7: Cold-start fallback and key expiry in the Scorer

**Files:**
- Modify: `detect/Scorer.java`
- Test: `detect/ScorerFallbackTest.java`, `detect/ScorerExpiryTest.java`

**Interfaces:**
- Consumes: Task 6's `Scorer`, `PopulationBaseline`, `Keys.split`.
- Produces: no new signatures. `Scorer` gains constant `EXPIRY_SLACK_MS = 120_000L`.

**Fallback.**
- Broadcast state `POPULATION` is keyed `scope + "|" + bucket + "|" + Math.floorMod(week, 2)`, so it holds this week's and next week's baseline for each scope and bucket. `processBroadcastElement` puts the record under its key.
- Lookup for `(scope, bucket, currentWeek)`: of the two entries, the one with the greatest `week <= currentWeek`; none if both are later.
- In Task 6's step 3, for `ERROR_RATE` and `LATENCY_P95` only: when `cache.own(s)` is `null`, use the lookup for `a.region()`, then for `PopulationBaseline.GLOBAL`. `ScoreEvent.source` is `"region"` or `"global"`. With neither, the minute is unscored for that signal.
- `VOLUME_5M` never uses the broadcast state.

**Expiry.**
- State: `last-active-ms` (`ValueState<Long>`, the latest minute with `count > 0`, or the first minute seen) and `expiry-timer-ms` (`ValueState<Long>`).
- `processElement`: update `last-active-ms`. If `expiry-timer-ms` is empty, register an event-time timer at `lastActive + expiry + EXPIRY_SLACK_MS` and store it. One timer per key at a time; zero-count minutes don't move it.
- `onTimer(t)`:
  1. If `ctx.currentWatermark() == Long.MAX_VALUE`, return. The end of a bounded replay is not a week of silence.
  2. `due = lastActive + expiry + EXPIRY_SLACK_MS`. If `t < due`, register and store a timer at `due` and return.
  3. Otherwise, for every machine in `machines` that is firing, emit a `resolved` `AlertEvent`: detector, setting and signal from the machine id split on `/`; tenant, region and unit from `Keys.split(ctx.getCurrentKey())`; `startsAtMs` and `alertId` from the machine's `startedAtMs`; `eventTimeMs = t`; `value` and `reference` 0.0; `reason "expired"`. Then clear all six states.
- The slack keeps the Scorer's state until after `MinuteAggregator`'s own last zero minute (at most `lastEvent + 7 d`), so no late minute recreates state.

- [ ] **Step 1: Write the failing tests** (harness and helpers as in Task 6; `emitScores = true`)
  - `ScorerFallbackTest.coldKeyIsUnscoredWithoutFallback`: at `MON`, 3 minutes with count 100, errors 10, no broadcast → no events; the `error_rate` scores are `null`.
  - `fallsBackToRegion`: broadcast `PopulationBaseline("eu", 0, 1, 0.01, 0.002, 100, 4, 500)`, then the same 3 minutes → one seasonal `error_rate` firing event; the score is ≈ 9.045 (±0.01) and `source == "region"`.
  - `fallsBackToGlobalWithoutRegion`: the same with scope `"*"` → fires, `source == "global"`. With both broadcast, `source == "region"`.
  - `latencyFallsBackToo`: region baseline with latency median 100 and scaled MAD 4; p95 120 for 3 minutes → one `latency_p95` firing event (spread 5, score 4).
  - `fallbackForNextWeekIsNotUsedEarly`: broadcast only `("eu", 0, 2, …)`; spike at `MON` (week 1) → no events. Then broadcast `("eu", 0, 1, …)` too and spike at `MON + 10 min` → fires.
  - `olderFallbackIsUsedWhenThisWeeksIsMissing`: broadcast only `("eu", 0, 1, …)`; spike at `MON + 2·WEEK` (week 3, key still cold) → fires.
  - `ownBaselineWinsOnceWarm`: two weeks of history with error rate 0, the region baseline above broadcast for weeks 1–3; at `W3` the `error_rate` `ScoreEvent` has `source == "own"` and `median == 0.0`.
  - `volumeNeverUsesFallback`: region baseline broadcast; a cold key with count 100 for 10 minutes then count 30 for 10 → no `volume_5m` event; `volume_5m` scores have `median == null`.
  - `baselinesSurviveSnapshotAndRestore`: two weeks of history and one broadcast record; `snapshot(1, 1)`; a new harness with `initializeState(snapshot)` and `open()`; the spike at `W3` fires with the same `alertId` and reference as `errorRateFiresAgainstOwnBaseline`, and a cold key in the restored harness still finds the broadcast record.
  - `stateTypesArePojos`: `TypeInformation.of(X.class)` is a `PojoTypeInfo` for `BucketHistory`, `HourCache`, `VolumeRing`, `AlertStateMachine`, `PopHistogram` and `MinuteAccumulator`.
  - `ScorerExpiryTest.expiresAfterSevenDaysAndResolvesFiringAlerts`: demo thresholds; minutes 0, 1, 2 with count 100, errors 10 (static alert fires). `watermark(604_919_999)` → nothing new. `watermark(605_040_000)` → one `resolved` event with `reason "expired"`, `eventTimeMs == 605_040_000`, the firing event's `alertId`, and no non-finite number. A minute fed afterwards behaves like a new key (the 5-minute sum is not available for 4 more minutes).
  - `zeroMinutesDoNotPostponeExpiry`: as above, but zero-count minutes keep arriving up to minute 10 079 → at `605_040_000` every firing alert gets one `expired` event: the `error_rate` alert and the static `volume_5m` alert the silence raised.
  - `activityPostponesExpiry`: a minute with count 1 at day 6 → no expiry at `605_040_000`; `numEventTimeTimers() == 1`.
  - `doesNotExpireAtFinalWatermark` (`@Timeout(2)`): firing alert, then `watermark(Long.MAX_VALUE)` → no `resolved` event.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java`
- [ ] **Step 3: Implement fallback and expiry.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures.
- [ ] **Step 5: Commit.** `git add flink-job && git commit -m "Add region and global fallback baselines and event-time key expiry"`

### Task 8: Job wiring, scores to ClickHouse and Grafana, forwarder on the seasonal setting

**Files:**
- Modify: `JobConfig.java`, `Pipeline.java`, `TenantBaselinesJob.java`, `io/JsonSerialization.java`
- Modify: `infra/clickhouse/init/01-schema.sql`, `infra/grafana/dashboards/tenant-baselines.json`, `docker-compose.yml`, `py/src/tenant_baselines/forwarder.py`
- Test: `JobConfigTest.java` (new), `io/JsonSerializationTest.java`, `PipelineTest.java`, `TenantBaselinesJobTest.java`, `py/tests/test_forwarder.py`

**Interfaces:**
- Consumes: `Scorer`, `PopulationBaselines`, `ScoreEvent`, `SeasonalSetting`.
- Produces:
  - `record JobConfig(String bootstrap, String ns, String runId, String mode, String thresholdsPath, long replayEndMs, boolean bounded, String groupId, List<SeasonalSetting> seasonal, boolean emitScores)`.
    - New flags: `--seasonal-k 4` (comma-separated; each becomes `SeasonalSetting.ofK`) and `--scores true|false` (default: true only when `ns` is `live`).
    - New helper `scoresTopic()` = `ns + ".scores"`.
  - `Pipeline.Outputs(DataStream<MinuteAgg> aggregates, DataStream<AlertEvent> alerts, DataStream<ScoreEvent> scores, DataStream<Event> late)`. `Pipeline.build` keeps its signature.
  - `JsonSerialization.scoreKey()`: `SerializationSchema<ScoreEvent>` of the UTF-8 `key`.
  - In `Pipeline.build`, Task 6's topology stays; the Scorer is now built with `cfg.seasonal()` and `cfg.emitScores()`, and `scores = alerts.getSideOutput(Scorer.SCORES)`.
  - `TenantBaselinesJob.addTo` sinks `scores` to `cfg.scoresTopic()` with uid and name `scores-sink` (at least once, like the others), only when `cfg.emitScores()`.
- **ClickHouse** (added to `01-schema.sql`, same patterns as the existing tables):
  - `tb.kafka_scores`: every `ScoreEvent` JSON field (`run_id, key, tenant, region, unit, signal String; minute_ms Int64; value, median, spread, score Nullable(Float64); source Nullable(String)`); topics `live.scores,eval.scores,bench.scores`; group `ch-scores`.
  - `tb.scores`: `ReplacingMergeTree ORDER BY (ns, run_id, key, signal, minute)`, with `minute DateTime64(3, 'UTC')`, LowCardinality strings, the four `Nullable(Float64)` and `source LowCardinality(Nullable(String))`.
  - `tb.mv_scores` and `tb.mv_scores_errors`.
- **Grafana:** panel 1 becomes one panel definition repeated per signal, which is how "one key's three signals with the baseline band" fits three different units.
  - New variable `signal`: custom, values `error_rate,latency_p95,volume_5m`, multi-value, all selected, hidden.
  - Panel 1: `"repeat": "signal"`, `"repeatDirection": "v"`, height 8, title `$key: $signal with baseline band`, query
    ```sql
    SELECT minute AS time, value, median, median + 4 * spread AS upper,
           greatest(median - 4 * spread, 0) AS lower
    FROM tb.scores FINAL
    WHERE ns = 'live' AND key = '${key}' AND signal = '${signal}'
      AND minute >= $__fromTime AND minute <= $__toTime
    ORDER BY time
    ```
  - Overrides: `upper` and `lower` drawn with line width 0; `upper` has `custom.fillBelowTo: "lower"` and fill opacity 15; `median` dashed. The static-threshold overrides go away.
  - The alert-log panel stays, moved below. Dashboard default time range `now-3h` to `now`.
- **Forwarder:** `--live-detector` defaults to `seasonal` and `--live-setting` to `k4`; the compose command passes the same two values.

- [ ] **Step 1: Write the failing tests**
  - `JobConfigTest.defaults`: `parse(new String[0])` has `seasonal == List.of(new SeasonalSetting("k4", 4, 2))`, `emitScores == true`, `scoresTopic() == "live.scores"`.
  - `JobConfigTest.seasonalGridAndScoresFlag`: `--ns eval --seasonal-k 3,4,5,6` → 4 settings named `k3`…`k6` with `resolveK` 1.5, 2, 2.5, 3 and `emitScores == false`; adding `--scores true` → true.
  - `JsonSerializationTest.scoreEventWritesNulls`: a `ScoreEvent` with null `median`, `spread`, `score`, `source` serializes with keys `run_id, key, tenant, region, unit, signal, minute_ms, value, median, spread, score, source` and JSON `null` for those four.
  - `PipelineTest.scoresCoverEveryKeyMinuteAndSignal`: the existing 15-minute input with `emitScores = true` → 45 `ScoreEvent`s. The two existing tests still pass (15 minutes of data leaves the seasonal detector cold, so the static alert is still the only one).
  - `PipelineTest.seasonalAlertFiresAfterTwoWeeksOfHistory`: one key, 25 evenly spaced ok events per minute from `MON` for two weeks and 10 minutes, then 5 minutes where every second event is an error; `replayEndMs` at the end. The alerts contain exactly one `firing` event with `detector "seasonal"` and `signal "error_rate"`, starting at the first error minute. (No fallback assertion here: a MiniCluster run can advance the watermark by weeks in one step, see Review Focus 3. The harness tests cover fallback.)
  - `TenantBaselinesJobTest.everyOperatorOfTheLiveJobHasAUid` still passes with the new operators.
  - `test_forwarder.py::test_resolve_is_due_immediately`: fire, `mark_sent`, then a resolved event 10 s later → `due()` returns the entry with `endsAt` at once.
  - `test_forwarder.py::test_stale_boundary_is_600s`: `emitted_at_ms − event_time_ms == 600_000` is kept; `600_001` is skipped.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java`; `make test-py`
- [ ] **Step 3: Implement the Java wiring, the schema, the dashboard and the forwarder defaults.**
- [ ] **Step 4: Run the tests.** `make test` → 0 failures, ruff clean.
- [ ] **Step 5: Check the infra on a fresh stack.**
  Run: `make reset && make jar && make up`
  Run: `curl -s -u tb:tb-local 'http://localhost:8123/?query=SHOW%20TABLES%20FROM%20tb'`
  Expected: the milestone 1 tables plus `kafka_scores`, `scores`, `mv_scores`, `mv_scores_errors`.
  Run: `curl -s localhost:3000/api/dashboards/uid/tenant-baselines | python3 -c "import sys,json; d=json.load(sys.stdin)['dashboard']; print([p.get('repeat') for p in d['panels']])"`
  Expected: `['signal', None]`.
- [ ] **Step 6: Commit.** `git add -A && git commit -m "Wire PopulationBaselines and scores through the job, ClickHouse and Grafana"`

### Task 9: Generator on one continuous timeline

**Files:**
- Modify: `py/src/tenant_baselines/gen_live.py`, `py/src/tenant_baselines/incidents.py`, `docker-compose.yml` (generator command)
- Test: `py/tests/test_gen_live.py`, `py/tests/test_incidents.py`

**Interfaces:**
- Consumes: Task 1's `Incident`, `key_rate_rpm`; `World`, `Key`, `Unit.dedicated_for`.
- Produces:
  - `LiveGenerator.minute(minute_ms: int) -> list[tuple[str, bytes]]`: every key's events for `[minute_ms, minute_ms + 60_000)`, sorted by timestamp. Per key: `n ~ Poisson(key rate × volume_mult)`, timestamps uniform in the minute, latency and `ok` as in `tick`, with load and incident effects taken at the minute's start.
  - `LiveGenerator.set_incidents(incidents: list[Incident]) -> None`.
  - `LiveGenerator.partition_of(key: str, n_partitions: int) -> int`: the key's index in `world.keys` modulo `n_partitions`.
  - `gen_live.next_step(next_ms: int, now_ms: int) -> Literal["minute", "second", "wait"]`: `"minute"` when `next_ms % 60_000 == 0 and next_ms + 60_000 <= now_ms`; else `"second"` when `next_ms + 1000 <= now_ms`; else `"wait"`.
  - `incidents.pick_drop_key(world: World, t_ms: int) -> Key`: the key on a dedicated unit with the highest `key_rate_rpm` at `t_ms` (first in world order on a tie).
  - `incidents.pick_spike_region(world: World, t_ms: int) -> str`: the region with the most keys at `key_rate_rpm >= 25` at `t_ms` (first in world order on a tie).
  - `incidents.demo_script(world: World, t0_ms: int) -> list[Incident]`:
    1. `Incident("volume_mult", t0 + 2 min, t0 + 12 min, 0.1, key=pick_drop_key(world, t0 + 2 min).key)`
    2. `Incident("error_spike", t0 + 14 min, t0 + 24 min, 0.20, region=pick_spike_region(world, t0 + 14 min))`
  - CLI: new `--history-days` (default 21; 0 gives milestone 1's live-only behaviour) and `--partitions` (default 8).

**`main` loop:**
- `next_ms` starts at `now − history_days` days, rounded down to a whole minute. The generator starts with no incidents.
- Each turn calls `next_step(next_ms, now)`: `"minute"` produces `gen.minute(next_ms)` and adds 60 000; `"second"` produces `gen.tick(next_ms)` and adds 1000; `"wait"` sleeps until `next_ms + 1000`.
- The first `"wait"` is catch-up. With `--script demo` the generator then calls `set_incidents(demo_script(world, next_ms))` and logs one line: `caught up after <s> s, <n> events; demo script: volume x0.1 on <key> <from>-<to> UTC, error spike in <region> <from>-<to> UTC`.
- Every message is produced with `partition=gen.partition_of(key, args.partitions)`, so each partition's events are in time order and every partition of the demo topic has data from the first minutes. No `timestamp=` argument.
- During history it logs progress every 10 s: the event time reached, the percentage of the history done and events per second.
- SIGINT and SIGTERM call `producer.flush(10)` and exit 0.

The dedicated-unit drop goes to 10%, not §5.4's 30%: with the demo world a 30% drop scores under 4 in ramp hours and on weekend nights, and the demo has to work whenever it is run. Both targets are picked at catch-up for the same reason; a fixed region has no key above minimum evidence on weekend nights.

- [ ] **Step 1: Write the failing tests**
  - `test_gen_live.py::test_minute_same_seed_same_output`.
  - `test_minute_sorted_and_inside_minute`: timestamps non-decreasing and in `[m, m + 60_000)`.
  - `test_minute_count_matches_rate`: over 120 consecutive minutes at a weekday peak hour, the event count is within 5% of `Σ key_rate_rpm`.
  - `test_minute_applies_volume_mult`: with `Incident("volume_mult", …, 0.1, key=k)` set through `set_incidents`, key `k`'s count over 60 minutes is within 30% of a tenth of its rate, and other keys are unaffected (within 10% of their rate).
  - `test_partition_of_covers_all_partitions`: for the demo world, `{partition_of(k.key, 8) for k in keys} == set(range(8))`, and the same key always gives the same partition.
  - `test_next_step`: `(120_000, 180_000)` → `"minute"`; `(120_000, 179_999)` → `"second"`; `(121_000, 400_000)` → `"second"`; `(120_000, 120_999)` → `"wait"`.
  - `test_incidents.py::test_demo_script_shape`: two incidents; the first is `volume_mult` 0.1 on a key whose unit has `dedicated_for` set, 10 minutes long; the second is an `error_spike` of 0.20 with a region, starting 2 minutes after the first ends.
  - `test_demo_script_targets_are_detectable_all_week`: for `t0` at every hour of one week, the drop key's rate at the drop's start is ≥ 7 rpm (5-minute expectation ≥ 35) and the spike region has ≥ 2 keys at ≥ 25 rpm at the spike's start.
- [ ] **Step 2: Run them and confirm they fail.** `make test-py`
- [ ] **Step 3: Implement.** In `docker-compose.yml` the generator command gains `--history-days 21 --partitions 8`.
- [ ] **Step 4: Run tests and lint.** `make test-py && make lint-py` → all pass.
- [ ] **Step 5: Commit.** `git add py docker-compose.yml && git commit -m "Generate three weeks of history at full speed, then continue live"`

### Task 10: `make demo` on the continuous timeline, smoke test, measurements

**Files:**
- Create: `scripts/demo.sh`
- Modify: `Makefile`, `scripts/smoke.sh`, `README.md`, `docs/superpowers/m1-followups.md`

**Behaviour:**
- **`make demo`** becomes `demo: jar up` followed by `scripts/demo.sh`, which:
  1. **Guard.** Reads `live.events` end offsets (`docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic live.events`, lines `topic:partition:offset`). If any offset is above 0 it prints `live.events already has data. The demo timeline cannot restart in place: run "make reset" first.` and exits 1.
  2. Starts the generator: `docker compose --profile demo up -d --build --no-deps generator`.
  3. Waits up to 120 s until all 8 partitions have an offset above 0, then runs the submit (`docker compose exec -T jobmanager bash /opt/tb-scripts/submit-job.sh`). The job must not start first: an empty split latches to the wall clock after 60 s and the history would be dropped as late.
  4. Shows progress every 5 s until the newest minute in `tb.aggregates` (`ns='live'`) is within 2 minutes of now: `warming baselines: 43% (history at 2026-10-28 14:00 UTC, 55 s)`. Gives up with exit 1 after 15 minutes.
  5. Prints `caught up after <N> s`, the three URLs, the generator's `demo script:` log line, and what to watch: the tenant drop starts about 2 minutes after catch-up and pages after about 5 to 8 more; the regional error spike starts about 14 minutes after catch-up and pages about 4 minutes later; history alerts appear in the alert log but are never paged.
- **`scripts/smoke.sh`** (still run by `make smoke` after `make demo`; default `LIMIT_S=1800`). Polled checks:
  1. `tb.aggregates` has live rows and `min(minute) <= now() - INTERVAL 20 DAY`.
  2. `tb.alert_log FINAL` has a `firing` row with `detector='seasonal' AND signal='volume_5m' AND unit LIKE '%-d-%' AND dateDiff('second', event_time, emitted_at) < 600` (decided live, not in the history).
  3. `tb.alert_log FINAL` has a `firing` row with `detector='seasonal' AND signal='error_rate'` and the same `dateDiff` condition.
  4. Alertmanager has alerts for `filter=detector%3D%22seasonal%22`.
  5. `pager-sink` logged a notification.
  6. `tb.scores FINAL` has live rows with `median IS NOT NULL` for each of the three signals.

  Then, once: 7. `tb.ingest_errors` is empty. 8. The restore check as today, plus `count() - uniqExact(key, signal, minute)` on `tb.scores FINAL` is 0.
- **`README.md`:** "milestone 2 of 6"; one sentence on the seasonal detector; the quickstart says `make demo` needs an empty stack (`make reset`) and how long warm-up takes (the measured figure); link to this plan.
- **`m1-followups.md`:** delete the items this milestone landed (the two spec-wording items, the `volume_drop` clamp, the duplicated rate formula, the `load` parameter name, flush and Ctrl-C handling, `make smoke` not enforcing a reset, the demo timing hint, and the tests: count = 20 boundary, calm-streak pause, `Level.judge` boundaries, immediate resolve send, the 600 s boundary). Everything else stays, including the per-key override and validity-window tests (milestone 4 touches that code).

- [ ] **Step 1: Run the full test suite.** `make test` → Java and Python 0 failures, ruff clean.
- [ ] **Step 2: Run CI locally.** `make ci` → both jobs exit 0.
- [ ] **Step 3: Run the end-to-end smoke test from empty volumes.**
  Run: `make reset && make smoke`
  Expected: `PASS` for all 8 checks. On a FAIL, debug with superpowers:systematic-debugging before changing code.
  Then run `make demo` again without a reset. Expected: the refusal message and exit code 1 (Review Focus 5).
- [ ] **Step 4: Record the measurements** (each is reported to the owner as measured on this machine, with the Colima size):
  - warm-up seconds, from `demo.sh`'s `caught up after` line, and the generator's own catch-up seconds and event count from its log;
  - `lateEvents` from the Flink REST metrics of the `late-events` operator (expected 0 or a handful at the catch-up seam; report the number);
  - the size of the latest completed checkpoint from `GET :8081/jobs/<id>/checkpoints` (estimate for 18 keys: about 11 MB of key history plus about 5 MB in `PopulationBaselines`);
  - `docker stats --no-stream` for the TaskManager (must stay under 2560m) and `docker inspect -f '{{.RestartCount}} {{.State.OOMKilled}}'` for it (expected `0 false`).
  - If warm-up took more than 4 minutes, stop here and report the figures with Q1's options; do not tune.
- [ ] **Step 5: Put the measured warm-up into the hint.** `demo.sh`'s opening line and the README say "about N minutes", with N the Step 4 figure rounded to the half minute.
- [ ] **Step 6: Check by hand in the browser.** The Grafana dashboard shows three band panels for the dropped key, with the value leaving the band during the drop; the alert log lists both detectors; Alertmanager shows only `seasonal` alerts, none from the history.
- [ ] **Step 7: Do the repo hygiene check.**
  Run: `git grep -n -i -E 'spearhead|leverag|synerg|cutting-edge|results-driven|passionate'`
  Expected: no matches outside `docs/superpowers/`.
- [ ] **Step 8: Commit and stop.**
  `git add -A && git commit -m "Milestone 2: seasonal baselines on one continuous demo timeline"`
  Then stop for the owner's review. Report the smoke output and the Step 4 measurements, and say plainly that the GitHub Actions workflow hasn't run on GitHub.
