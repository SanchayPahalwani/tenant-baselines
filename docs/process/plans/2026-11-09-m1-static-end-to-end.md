# tenant-baselines Milestone 1 Implementation Plan: end to end with static thresholds

**Goal:** `docker compose up` + `make demo` runs the live pipeline end to end:
1. A Python generator writes events.
2. The Flink job aggregates them per minute.
3. A global static detector fires and resolves alerts.
4. Aggregates and alerts land in ClickHouse and Grafana.
5. Alerts reach Alertmanager and a webhook pager.

**Architecture:** The Flink 2.2.1 Java job (`flink-job/`) reads `live.events` and runs `MinuteAggregator` (keyed by key), then `Scorer` (keyed by key). It writes `live.aggregates` and `live.alerts` at least once. ClickHouse pulls both topics through Kafka-engine tables into `ReplacingMergeTree` tables. A Python forwarder (`py/`) re-sends firing alerts to Alertmanager, which routes them to `pager-sink`.

**Tech stack:**
- Java 17 and Maven, Flink 2.2.1, flink-connector-kafka 5.0.0-2.2, Jackson 2.18.12, JUnit 5.11.4, AssertJ 3.27.7.
- Python 3.12 with uv: confluent-kafka 2.16.1, orjson 3.13.0, numpy 2.5.4, httpx 0.28.1, pytest 9.2.0, ruff 0.17.4.
- Docker images: Kafka 4.4.0, ClickHouse 26.8.36.3, Alertmanager v0.34.2, Grafana 13.2.5 with grafana-clickhouse-datasource 4.23.4.

**Spec:** `docs/process/specs/2026-11-09-tenant-baselines-design.md` (v2, approved). Read §4.1, §4.2, §4.5, §4.9, §4.10 and §7 before starting.

## Global Constraints

- Java package root: `io.github.sanchaypahalwani.tenantbaselines` (written `<pkg>` below). Java 17 (`maven.compiler.release=17`).
- **Images:** `flink:2.2.1-java17`, `apache/kafka:4.4.0`, `clickhouse/clickhouse-server:26.8.36.3`, `prom/alertmanager:v0.34.2`, `grafana/grafana:13.2.5`, `maven:3.10.0-eclipse-temurin-17-noble`, `python:3.12-slim`. The uv binary comes from `ghcr.io/astral-sh/uv:0.13.0`.
- **Flink test dependencies:** `org.apache.flink:flink-test-utils:2.2.1` (test scope) and `org.apache.flink:flink-runtime:2.2.1` with `<type>test-jar</type>` (test scope). In 2.2.1 the harness classes `org.apache.flink.streaming.util.ProcessFunctionTestHarnesses` and `KeyedOneInputStreamOperatorTestHarness` live in flink-runtime's test jar. `ProcessFunctionTestHarnesses.forKeyedProcessFunction(fn, keySelector, keyType)` already calls `open()`.
- **Flink scopes:** Flink core and streaming dependencies are `provided`. The connector and Jackson are bundled by the shade plugin.
- **Flink 2.x config:** set through `FLINK_PROPERTIES` (newline-separated `key: value`); `flink-conf.yaml` is gone. Use `KeyedProcessFunction.open(OpenContext)` and `java.time.Duration` everywhere (no `Time`).
- **Watermarks:**
  - Replay: `forBoundedOutOfOrderness(20 s)`, `withIdleness(30 s)`, `withWatermarkAlignment("events", 1 min, 1 s)`.
  - Live: a latching wall-clock generator with a 20 s bound and a 60 s catch-up window, and **no idleness**.
- **Detection constants:** a scored minute needs ≥ 20 requests. Fire after 3 consecutive breaching scored minutes; resolve after 5 consecutive calm scored minutes; unscored minutes pause both streaks. Volume is the trailing 5-minute sum of counts.
- **Kafka output** is at least once (`DeliveryGuarantee.AT_LEAST_ONCE`). Alert IDs are deterministic. Kafka has `auto.create.topics.enable=false`, so topics are created by `kafka-init`.
- **ClickHouse:** database `tb`, user `tb`, password `tb-local` (local demo credential, documented as such). Kafka-engine tables use `kafka_handle_error_mode='stream'` and list every JSON field the job writes. Epoch ms are converted with `fromUnixTimestamp64Milli(x, 'UTC')`.
- **Grafana:** plugin installed via `GF_PLUGINS_PREINSTALL_SYNC=grafana-clickhouse-datasource@4.23.4`. `GF_INSTALL_PLUGINS` is a no-op in 13.x.
- **Storage:** Kafka, ClickHouse, Flink checkpoints, Grafana and pager logs all use named volumes (no bind mounts), except read-only config and jar mounts. Memory limits must total ≤ 10 GB.
- **Repo rules:** Apache-2.0. No employer, client or company names; no marketing vocabulary; synthetic data labelled synthetic.
- **Git:** commit locally after each task. Never add a remote or push.

## Review Focus

1. **Malformed or partial JSON on `live.events`:** the job keeps running, drops the record and counts it in the `malformedEvents` metric. Pinned in Task 1 (`EventDeserializationSchemaTest`).
2. **Restarted job re-emitting minutes and alerts:** `FINAL` queries show one row per `(key, minute)` and per `(alert_id, state)`, and the forwarder doesn't post a second alert. Pinned in Task 8 (`test_duplicate_fire_is_idempotent`) and Task 10 (the smoke step resubmits the job).
3. **Alertmanager down or returning 5xx:** the forwarder keeps its state, retries on the next tick and doesn't crash. Pinned in Task 8 (`test_failed_post_is_retried`).
4. **All traffic stops in live mode:** the watermark keeps following the wall clock, zero minutes keep coming, and the volume alert fires. Pinned in Task 4 (`keepsAdvancingWithWallClockWhenSilent`) and Task 5 (`volumeDownFiresOnSilence`).
5. **A Kafka partition that never receives an event:** the live watermark doesn't stick at `Long.MIN_VALUE`. Pinned in Task 4 (`emptySplitLatchesAfterCatchUpWindow`).

## File map

```
LICENSE  .gitignore  README.md  Makefile  docker-compose.yml  .github/workflows/ci.yml
shared/latency_bounds_ms.txt                      p95 bucket bounds, shared by Java and Python
scripts/wait-ready.sh  scripts/smoke.sh  scripts/submit-job.sh
flink-job/pom.xml
flink-job/src/main/java/<pkg>/
  model/Event.java  model/MinuteAgg.java  model/AlertEvent.java  model/Signal.java  model/Keys.java
  io/EventJson.java  io/EventDeserializationSchema.java  io/JsonSerialization.java
  agg/LatencyHistogram.java  agg/MinuteAccumulator.java  agg/MinuteAggregator.java
  time/LiveWatermarkGenerator.java  time/Watermarks.java
  detect/Judgement.java  detect/AlertStateMachine.java  detect/MinuteSignals.java  detect/VolumeRing.java
  detect/Level.java  detect/ThresholdSet.java  detect/ThresholdsFile.java  detect/AlertIds.java  detect/Scorer.java
  JobConfig.java  Pipeline.java  TenantBaselinesJob.java
flink-job/src/test/java/<pkg>/...                 one test class per production class named in the tasks
infra/kafka/create-topics.sh
infra/flink/thresholds-demo.json
infra/clickhouse/init/01-schema.sql
infra/alertmanager/alertmanager.yml
infra/grafana/provisioning/datasources/clickhouse.yml
infra/grafana/provisioning/dashboards/provider.yml
infra/grafana/dashboards/tenant-baselines.json
py/pyproject.toml  py/uv.lock  py/Dockerfile
py/src/tenant_baselines/{__init__,world,traffic,events,incidents,gen_live,forwarder,pager_sink}.py
py/tests/test_{world,traffic,events,incidents,gen_live,forwarder,pager_sink}.py
```

---

### Task 1: Repo scaffold, Maven project, models and JSON

**Files:**
- Create:
  - `LICENSE` (the official text from `https://www.apache.org/licenses/LICENSE-2.0.txt`)
  - `.gitignore` (`target/`, `data/`, `.venv/`, `__pycache__/`, `.idea/`, `*.iml`, `.DS_Store`)
  - `Makefile` with `jar` and `test-java`
  - `flink-job/pom.xml`
  - `model/*`, `io/*`
- Test: `io/EventJsonTest.java`, `io/EventDeserializationSchemaTest.java`, `io/JsonSerializationTest.java`

**Interfaces (produces):**
- `record Event(long ts, String tenant, String region, String unit, double latencyMs, boolean ok)`, with `String key()` returning `Keys.of(tenant, region, unit)`.
- `final class Keys`:
  - `static String of(String tenant, String region, String unit)` joins with `|`;
  - `static String[] split(String key)` returns `{tenant, region, unit}`.
- `record MinuteAgg(String runId, String key, String tenant, String region, String unit, long minuteMs, long count, long errors, Double p95LatencyMs)`. `p95LatencyMs` is `null` when `count == 0`.
- `enum Signal { ERROR_RATE("error_rate", UP), LATENCY_P95("latency_p95", UP), VOLUME_5M("volume_5m", DOWN) }`, with `String wire()`, `Direction direction()` and nested `enum Direction { UP, DOWN }`.
- `record AlertEvent(String alertId, String runId, String detector, String setting, String scope, String key, String tenant, String region, String unit, String signal, String state, long startsAtMs, long eventTimeMs, long emittedAtMs, double value, double reference, String reason)`.
  - `state` is `"firing"` or `"resolved"`.
  - `eventTimeMs` is the end of the minute that decided the transition.
  - `reason` may be null.
- `final class EventJson`: `static Event parse(byte[] json)` reads the wire fields `ts, tn, rg, un, lat, ok`. It returns `null` for malformed JSON, missing fields or wrong types; it never throws.
- `EventDeserializationSchema implements DeserializationSchema<Event>`:
  - `deserialize(byte[])` returns `EventJson.parse(...)`. A null is skipped by the Kafka source.
  - It increments the counter `malformedEvents`, registered in `open(InitializationContext)`, on each null.
- `final class JsonSerialization`:
  - `static <T> SerializationSchema<T> json()`: a Jackson mapper with `PropertyNamingStrategies.SNAKE_CASE` that writes nulls as `null`;
  - `static SerializationSchema<MinuteAgg> aggKey()` and `static SerializationSchema<AlertEvent> alertKey()` return UTF-8 `key` bytes.

- [ ] **Step 1: Write the failing tests**
  - `EventJsonTest.parsesWireFormat`: `{"ts":60000,"tn":"t01","rg":"eu-central","un":"eu-central-s1","lat":83.2,"ok":false}` → `Event(60000,"t01","eu-central","eu-central-s1",83.2,false)`, and `key()` is `"t01|eu-central|eu-central-s1"`.
  - `EventJsonTest.returnsNullOnBadInput`: parameterized over `"not json"`, `"{}"`, `{"ts":"x",...}` and a missing `lat`. Each gives `null`.
  - `EventDeserializationSchemaTest.countsMalformed`: open it with a hand-written `DeserializationSchema.InitializationContext`. Its `getMetricGroup()` returns an `UnregisteredMetricsGroup` subclass whose `counter(String)` returns one shared `SimpleCounter`. Two bad payloads give counter 2 and `deserialize` returns `null` both times.
  - `JsonSerializationTest.writesSnakeCase`: serializing `MinuteAgg("live","k","t","r","u",0,0,0,null)` gives JSON with keys `run_id, key, tenant, region, unit, minute_ms, count, errors, p95_latency_ms`, and `p95_latency_ms` is `null`.
- [ ] **Step 2: Run them and confirm they fail**
  Run: `make test-java`
  Expected: compilation fails (the classes don't exist yet).
- [ ] **Step 3: Implement**
  - `pom.xml`:
    - Flink BOM-free explicit versions; Flink deps `provided`;
    - `maven-shade-plugin` 3.6.2 producing `target/tenant-baselines-job.jar` (main class `<pkg>.TenantBaselinesJob`, excluding `org.apache.flink:*` provided artifacts and `META-INF/*.SF|DSA|RSA`);
    - `maven-surefire-plugin` 3.6.1, `maven-compiler-plugin` 3.16.0;
    - extra resource directory `../shared`.
  - `Makefile`: `jar` and `test-java` run `docker run --rm -v $(PWD):/w -v tb-m2:/root/.m2 -w /w/flink-job maven:3.10.0-eclipse-temurin-17-noble mvn -B <package|verify>`.
- [ ] **Step 4: Run the tests and confirm they pass**
  Run: `make test-java`
  Expected: `BUILD SUCCESS` with 4+ tests run and 0 failures.
- [ ] **Step 5: Commit**
  `git add -A && git commit -m "Add Flink job skeleton, models and JSON codecs"`

### Task 2: Latency histogram with a shared bucket table

**Files:**
- Create: `shared/latency_bounds_ms.txt`, `agg/LatencyHistogram.java`
- Test: `agg/LatencyHistogramTest.java`

**Interfaces (produces):**
- `final class LatencyHistogram`:
  - `static final int BUCKETS = 202`;
  - `static double[] bounds()`, loaded once from classpath `/latency_bounds_ms.txt`;
  - `static int bucketOf(double latencyMs)`;
  - `static double representative(int bucket)`;
  - `static Double p95(int[] counts, long n)`, which returns `null` when `n == 0`.

The table is the source of truth, so Java and Python (milestone 4) bucket identically. Generate it once with `python3 -c "print('\n'.join(f'{round(0.5*1.06**i,3)}' for i in range(202)))" > shared/latency_bounds_ms.txt`, then commit it and never regenerate it.

The definitions the tests don't fully determine:
- `bucketOf`: the largest `i` with `bounds[i] <= latencyMs`, found by binary search. Below `bounds[0]` gives 0; the last bucket (201) is open-ended.
- `representative(i)`: `Math.sqrt(bounds[i] * bounds[i+1])` for `i < 201`, and `bounds[201]` for the last bucket.
- `p95`: `rank = (long) Math.ceil(0.95 * n)`; walk the cumulative counts to the first bucket where cumulative ≥ rank and return its representative.

- [ ] **Step 1: Write the failing tests**
  - `tableIsAscendingAndComplete`: length 202, `bounds()[0] == 0.5`, strictly increasing.
  - `bucketsClampAtBothEnds`: `bucketOf(0.01) == 0`, `bucketOf(1e9) == 201`.
  - `representativeWithinThreePercent`: for every `i < 201` and every `x` in `{bounds[i], bounds[i+1] − 1e-6}`, `|representative(i) − x| / x ≤ 0.03`.
  - `p95OfOneToHundred`: counts built from latencies 1..100 ms and n = 100 give `representative(bucketOf(95.0))`.
  - `p95OfEmptyIsNull`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java` → `LatencyHistogramTest` doesn't compile.
- [ ] **Step 3: Implement `LatencyHistogram` as specified above.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures.
- [ ] **Step 5: Commit.** `git commit -am "Add shared log-bucket latency histogram"` (`git add shared/` first).

### Task 3: MinuteAggregator with zero-fill, late side output, end-of-input guard and expiry

**Files:**
- Create: `agg/MinuteAccumulator.java`, `agg/MinuteAggregator.java`
- Test: `agg/MinuteAggregatorTest.java`

**Interfaces:**
- Consumes: `Event`, `MinuteAgg`, `Keys.split`, `LatencyHistogram`.
- Produces:
  - `class MinuteAggregator extends KeyedProcessFunction<String, Event, MinuteAgg>`, with constructor `(String runId, long replayEndMs, Duration expiry)`, where `replayEndMs == Long.MIN_VALUE` means "none".
  - `public static final OutputTag<Event> LATE = new OutputTag<>("late-events") {}`.
  - `MinuteAccumulator` is a mutable POJO: `long count; long errors; int[] buckets = new int[LatencyHistogram.BUCKETS]`.

**Behaviour:**
- **State:** `MapState<Long, MinuteAccumulator> open` and `ValueState<Long> lastEventMs`.
- **Minute and timer:** `minute = ts − Math.floorMod(ts, 60_000)`; the timer for a minute is `minute + 59_999`.
- **`processElement`:**
  - If `minute + 59_999 <= currentWatermark()` the event is late, so `ctx.output(LATE, e)` and return.
  - Otherwise accumulate, register the timer, and set `lastEventMs = max(...)`.
- **`onTimer(t)`:**
  1. `minute = t − 59_999`. Emit a `MinuteAgg` for that minute from `open.get(minute)`, or a zero minute with `p95 = null`. Remove the entry.
  2. If `minute + 60_000 − lastEventMs >= expiry.toMillis()`, clear all state and return.
  3. `next = t + 60_000`. Re-register `next` only if `currentWatermark() != Long.MAX_VALUE`, or if `replayEndMs != Long.MIN_VALUE && next <= replayEndMs − 1`. This is the end-of-input guard: Flink fires timers registered during a watermark advance in the same loop, verified in `InternalTimerServiceImpl.tryAdvanceWatermark`.

**Test harness:** use `ProcessFunctionTestHarnesses.forKeyedProcessFunction(new MinuteAggregator("t", Long.MIN_VALUE, Duration.ofDays(7)), Event::key, Types.STRING)`. Push elements with `processElement(e, e.ts())` and read with `extractOutputValues()`.

- [ ] **Step 1: Write the failing tests**
  - `aggregatesOneMinute`: events at 0 s, 10 s and 20 s with latencies 100, 200 and 300; the third has `ok = false`. `processWatermark(59_999)` gives exactly one output: `minuteMs 0, count 3, errors 1, p95 = representative(bucketOf(300))`.
  - `emitsZeroMinutesWhenKeyGoesSilent`: an event at 0, then `processWatermark(179_999)`, gives minutes 0, 60 000 and 120 000. The last two have count 0 and null p95.
  - `lateEventGoesToSideOutput`: an event at 0, `processWatermark(59_999)`, then an event at 30 000. `getSideOutput(LATE)` has 1 record, and the main output has one minute with count 1.
  - `stopsZeroFillAtFinalWatermark`: an event at 0, then `processWatermark(Long.MAX_VALUE)`, gives exactly 1 output. The test must also finish in under 2 s (`@Timeout(2)`).
  - `zeroFillsUpToReplayEnd`: `replayEndMs = 300_000`, an event at 0, then `Long.MAX_VALUE` gives 5 outputs; the last has `minuteMs == 240_000`.
  - `expiresAfterQuietPeriod`: `expiry = Duration.ofMinutes(10)`, an event at 0, then `processWatermark(3_600_000)` gives exactly 10 outputs (minutes 0 to 540 000). Then an event at 7 200 000 and `processWatermark(7_259_999)` give 1 more output with count 1.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java` → compilation failure.
- [ ] **Step 3: Implement as specified above.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures.
- [ ] **Step 5: Commit.** `git commit -am "Add MinuteAggregator with zero-fill and end-of-input guard"`

### Task 4: Watermark strategies

**Files:**
- Create: `time/LiveWatermarkGenerator.java`, `time/Watermarks.java`
- Test: `time/LiveWatermarkGeneratorTest.java`

**Interfaces (produces):**
- `class LiveWatermarkGenerator implements WatermarkGenerator<Event>`, with constructor `(LongSupplier clockMs, long boundMs, long catchUpMs)`.
- `final class Watermarks`:
  - `static WatermarkStrategy<Event> replay()`: bounded out-of-orderness of 20 s, `withIdleness(Duration.ofSeconds(30))` and `withWatermarkAlignment("events", Duration.ofMinutes(1), Duration.ofSeconds(1))`.
  - `static WatermarkStrategy<Event> live()`: `WatermarkStrategy.forGenerator(ctx -> new LiveWatermarkGenerator(System::currentTimeMillis, 20_000, 60_000))`, with no idleness.
  - Both use the timestamp assigner `(e, t) -> e.ts()`.

**Generator logic** (the tests pin the observable behaviour):
- **Fields:** `maxTs = Long.MIN_VALUE`, `createdAt = clock`, `latched = false`, `lastEmitted = Long.MIN_VALUE`.
- **`onEvent`:** `maxTs = max(maxTs, ts)`.
- **`onPeriodicEmit`:**
  1. Read `now`.
  2. Latch when either `(maxTs != MIN && maxTs >= now − catchUpMs)` or `(maxTs == MIN && now − createdAt >= catchUpMs)`. Once latched, it stays latched.
  3. `candidate = latched ? max(maxTs, now) − boundMs − 1 : (maxTs == MIN ? none : maxTs − boundMs − 1)`.
  4. Emit `new Watermark(candidate)` only if `candidate > lastEmitted`.

**Tests** use a fake clock (`AtomicLong`) and a recording `WatermarkOutput`.

- [ ] **Step 1: Write the failing tests**
  - `followsEventTimeDuringBackfill`: `now = 10^12`, events at 0 and 120 000 → the last emitted watermark is `99_999`.
  - `keepsAdvancingWithWallClockWhenSilent`: `now = 10^12`, an event at `now − 5_000`, emit → `now − 20_001`. Advance the clock by 10 minutes with no events and emit again → `now + 600_000 − 20_001`.
  - `emptySplitLatchesAfterCatchUpWindow`: no events and the clock advanced 61 s → it emits `now − 20_001`. At 59 s it emits nothing.
  - `neverDecreases`: an event far ahead, then the clock moves back → no lower watermark is emitted.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java`
- [ ] **Step 3: Implement both classes.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures.
- [ ] **Step 5: Commit.** `git commit -am "Add replay and live watermark strategies"`

### Task 5: Detection core and the Scorer (global static)

**Files:**
- Create: `detect/{Judgement,AlertStateMachine,MinuteSignals,VolumeRing,Level,ThresholdSet,ThresholdsFile,AlertIds,Scorer}.java`, `infra/flink/thresholds-demo.json`
- Test: `detect/AlertStateMachineTest.java`, `detect/MinuteSignalsTest.java`, `detect/ThresholdsFileTest.java`, `detect/ScorerTest.java`

**Interfaces:**
- Consumes: `MinuteAgg`, `AlertEvent`, `Signal`.
- Produces:
  - `enum Judgement { BREACH, CALM, NEITHER, UNSCORED }`.
  - `class AlertStateMachine` is a mutable POJO: `boolean firing; int breachStreak; int calmStreak; long breachStartMs; long startedAtMs`, with `FIRE_AFTER = 3` and `RESOLVE_AFTER = 5`. It has `Transition step(Judgement j, long minuteMs)` and the nested `enum Transition { NONE, FIRE, RESOLVE }`.
    - **Not firing:** `BREACH` increments `breachStreak` and sets `breachStartMs` on the first breach. At 3 it becomes firing with `startedAtMs = breachStartMs`, resets both streaks and returns `FIRE`. `CALM` or `NEITHER` resets `breachStreak`.
    - **Firing:** `CALM` increments `calmStreak`; at 5 it stops firing and returns `RESOLVE`. `BREACH` or `NEITHER` resets `calmStreak`.
    - `UNSCORED` changes nothing.
  - `class VolumeRing` is a POJO, `long lastMinuteMs = Long.MIN_VALUE; long[] counts = new long[5]; int filled`. `double push(long minuteMs, long count)` returns the 5-minute sum, or `NaN` until 5 consecutive minutes are in the ring. A gap (`minuteMs != lastMinuteMs + 60_000`) resets it.
  - `record MinuteSignals(double errorRate, double latencyP95, double volume5m)`: `static MinuteSignals of(MinuteAgg a, double volume5m)`, with `MIN_REQUESTS = 20`. Error rate and latency are `NaN` when `count < 20`; error rate is `errors/count`. Also `double get(Signal s)`.
  - `record Level(double fire, double resolve)`, with `Judgement judge(Signal s, double v)`. `NaN` gives `UNSCORED`.
    - UP: `v >= fire` is `BREACH`, `v < resolve` is `CALM`, else `NEITHER`.
    - DOWN: `v <= fire` is `BREACH`, `v > resolve` is `CALM`, else `NEITHER`.
  - `record ThresholdSet(String detector, String setting, long validFromMs, long validToMs, Map<Signal, Level> global, Map<String, Map<Signal, Level>> perKey)`, with `boolean activeAt(long minuteMs)` and `Level levelFor(String key, Signal s)` (per-key first, then global, else `null`, which judges as `UNSCORED`).
  - `ThresholdsFile`: `static List<ThresholdSet> load(InputStream)` reads `{"sets":[{"detector","setting","valid_from_ms","valid_to_ms"(nullable → Long.MAX_VALUE),"global":{"<signal wire>":{"fire","resolve"}},"per_key":{"<key>":{...}}}]}`.
  - `AlertIds`: `static String of(String detector, String setting, String scope, String subject, String signal, long startsAtMs)` returns the first 32 hex characters of the SHA-256 of the fields joined with `|`.
  - `class Scorer extends KeyedProcessFunction<String, MinuteAgg, AlertEvent>`, with constructor `(String runId, List<ThresholdSet> sets)`.
    - **State:** `ValueState<VolumeRing>` and `MapState<String, AlertStateMachine>`, keyed by `detector + "/" + setting + "/" + signal.wire()`.
    - **Per minute:** compute the signals. For each set active at `minuteMs`, and each `Signal`, call `step(level.judge(...))`. On `FIRE` or `RESOLVE`, emit an `AlertEvent` with:
      - `scope "tenant"`;
      - `alertId = AlertIds.of(detector, setting, "tenant", key, signal, machine.startedAtMs)`;
      - `eventTimeMs = minuteMs + 60_000`;
      - `emittedAtMs = System.currentTimeMillis()`;
      - `value` is the signal value and `reference` is `level.fire()`.
- `thresholds-demo.json` holds one set, `global_static/default`, valid from 0 with no end:
  - `error_rate` fire 0.05, resolve 0.025;
  - `latency_p95` fire 400, resolve 250;
  - `volume_5m` fire 0, resolve 5.

- [ ] **Step 1: Write the failing tests**
  - `AlertStateMachineTest`:
    - `firesAfterThreeBreaches`: B, B, B → NONE, NONE, FIRE, with `startedAtMs` = the first B's minute.
    - `neitherResetsBreachStreak`: B, B, N, B, B → no FIRE.
    - `unscoredPausesStreak`: B, U, B, B → FIRE.
    - `resolvesAfterFiveCalm`: once firing, C×4 then N then C×5 → RESOLVE on the last C only.
    - `refireGetsNewStart`: fire, resolve, then B×3 starting at minute 20 → FIRE with `startedAtMs` = minute 20.
  - `MinuteSignalsTest`:
    - `belowMinimumEvidenceIsUnscored`: count 19 → error rate and latency are `NaN`.
    - `errorRateIsFraction`: count 100, errors 10 → 0.1.
    - `VolumeRing` sums 5 consecutive minutes and resets on a gap.
  - `ThresholdsFileTest.loadsDemoFile`: loading `infra/flink/thresholds-demo.json` (copied to test resources by the pom's test-resource entry `../infra/flink`) gives one set, `levelFor("any", ERROR_RATE)` is `Level(0.05, 0.025)`, and `validToMs` is `Long.MAX_VALUE`.
  - `ScorerTest`, using the harness keyed by `MinuteAgg::key` and the demo thresholds:
    - `errorRateFiresAndResolves`: minutes 0, 1 and 2 with count 100 and errors 10 → one firing event with `alertId == AlertIds.of("global_static","default","tenant",key,"error_rate",0)`, `eventTimeMs == 180_000`, value 0.1 and reference 0.05. Minutes 3 to 7 with errors 0 → one resolved event with the same `alertId`.
    - `volumeDownFiresOnSilence`: minutes 0–4 with count 10, then minutes 5–16 with count 0 → one firing `volume_5m` event with `startsAtMs == 540_000` and `eventTimeMs == 720_000`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-java`
- [ ] **Step 3: Implement all classes as specified above.**
- [ ] **Step 4: Run the tests and confirm they pass.** `make test-java` → 0 failures.
- [ ] **Step 5: Commit.** `git commit -am "Add alert state machine, static thresholds and Scorer"`

### Task 6: Pipeline wiring, job entry point and a MiniCluster test

**Files:**
- Create: `JobConfig.java`, `Pipeline.java`, `TenantBaselinesJob.java`
- Test: `PipelineIT.java` (run by surefire; name it `PipelineTest` if surefire's default includes are kept)

**Interfaces:**
- Consumes: everything from Tasks 1–5.
- Produces:
  - `record JobConfig(String bootstrap, String ns, String runId, String mode, String thresholdsPath, long replayEndMs, boolean bounded, String groupId)`, with `static JobConfig parse(String[] args)`.
    - Flags: `--bootstrap kafka:19092`, `--ns live`, `--run-id live`, `--mode live|replay`, `--thresholds /opt/tb-config/thresholds-demo.json`, `--replay-end-ms` (default `Long.MIN_VALUE`), `--bounded` (default false), `--group-id tb-<ns>`.
    - Topic helpers: `eventsTopic()` is `ns + ".events"`, and likewise `aggregatesTopic()` and `alertsTopic()`.
  - `final class Pipeline`:
    - `record Outputs(DataStream<MinuteAgg> aggregates, DataStream<AlertEvent> alerts, DataStream<Event> late)`;
    - `static Outputs build(DataStream<Event> timestampedEvents, JobConfig cfg, List<ThresholdSet> sets)` does `keyBy(Event::key)` → `MinuteAggregator(cfg.runId(), cfg.replayEndMs(), Duration.ofDays(7))` with uid `minute-aggregator`, then `keyBy(MinuteAgg::key)` → `Scorer` with uid `scorer`. `late` is the `MinuteAggregator.LATE` side output.
  - `TenantBaselinesJob.main(String[])`:
    - parses the config and loads thresholds from the file path (client side; the sets ship inside the operators);
    - builds a `KafkaSource<Event>` with `setValueOnlyDeserializer(new EventDeserializationSchema())`, `setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))` and `setBounded(OffsetsInitializer.latest())` when `bounded`;
    - calls `env.fromSource(source, mode.equals("live") ? Watermarks.live() : Watermarks.replay(), "events")`, then `Pipeline.build`;
    - sinks aggregates and alerts through `KafkaSink` with `DeliveryGuarantee.AT_LEAST_ONCE`, key serializers from `JsonSerialization` and value `JsonSerialization.json()`;
    - maps `late` through a counter `lateEvents` into `org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink`;
    - uses job name `"tenant-baselines-" + ns`.

- [ ] **Step 1: Write the failing test.** `PipelineIT` registers `@RegisterExtension static MiniClusterExtension` (1 TaskManager, 2 slots) and uses demo thresholds, `replayEndMs = 15 min`, and `env.fromData(events).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner((e, t) -> e.ts()))`.
  - `errorSpikeProducesOneFiringAlert`: one key gets 100 events a minute (evenly spaced, all ok) for minutes 0–9, then 100 a minute with every second event an error for minutes 10–14. `Pipeline.build(...).alerts().executeAndCollect()` must contain exactly one `firing` `error_rate` event, with `startsAtMs == 600_000`, and no other firing events.
  - `aggregatesStopAtReplayEnd`: the same input; `aggregates().executeAndCollect()` returns exactly 15 records, with minutes 0 through 840 000.
- [ ] **Step 2: Run it and confirm it fails.** `make test-java`
- [ ] **Step 3: Implement the three classes.**
- [ ] **Step 4: Run the tests and confirm they pass**, then build the jar.
  Run: `make test-java && make jar`
  Expected: 0 failures, and `flink-job/target/tenant-baselines-job.jar` exists.
  Run: `unzip -l flink-job/target/tenant-baselines-job.jar | grep -c 'org/apache/flink/connector/kafka'`
  Expected: greater than 0.
- [ ] **Step 5: Commit.** `git commit -am "Wire the Flink pipeline and Kafka source and sinks"`

### Task 7: Python project and live event generator

**Files:**
- Create: `py/pyproject.toml` (with the versions from Tech stack), `py/uv.lock`, `py/src/tenant_baselines/{__init__,world,traffic,events,incidents,gen_live}.py`
- Test: `py/tests/test_{world,traffic,events,incidents,gen_live}.py`
- Modify: `Makefile`, adding `test-py` (`cd py && uv run pytest -q`) and `lint-py` (`cd py && uv run ruff check . && uv run ruff format --check .`)

**Interfaces (produces):**
- `world.py`:
  - `@dataclass(frozen=True)` classes: `Region(name: str, utc_offset_h: int)`, `Tenant(name: str, home_region: str, tz_offset_h: int, peak_rpm: float)`, `Unit(name: str, region: str, dedicated_for: str | None, base_latency_ms: float)`, `Key(tenant: str, region: str, unit: str)` with property `key` equal to `f"{tenant}|{region}|{unit}"`, and `World(regions, tenants, units, keys)`.
  - `build_world(name: str, seed: int) -> World` supports `"demo"`:
    - regions `us-east` (−5), `eu-central` (+1) and `ap-southeast` (+8);
    - 8 tenants named `t01`–`t08`, with `peak_rpm` log-normal (median 150, σ 0.8, seeded);
    - 2 shared units per region named `<region>-s1/s2`, plus dedicated units `<region>-d-<tenant>` for 2 tenants;
    - about 20 keys.
  - `tz_offset_h` equals the home region's offset, except one tenant that differs.
- `traffic.py`:
  - `shape(local_hour: float, weekday: int, amplitude: float = 6.0) -> float` returns the fraction of peak:
    - piecewise linear: `1/amplitude` on [0, 7), rising to 1 by 9, 1 on [9, 17), falling to `1/amplitude` by 20, then `1/amplitude` until 24;
    - weekend days (5, 6) multiply by 0.3;
    - Monday 08:00–11:00 multiplies by 1.25.
  - `rate_rpm(tenant: Tenant, t_ms: int) -> float` returns `peak_rpm * shape(...)` in the tenant's local time.
  - `error_prob(load: float) -> float` returns `0.003 + 0.002 * load`.
  - `latency_median_ms(base: float, load: float) -> float` returns `base * (1 + 0.6 * load**2)`, where `load` is the `shape` value (0–1.25). Latency σ (log) is 0.5.
- `events.py`: `encode(ts_ms: int, tenant: str, region: str, unit: str, latency_ms: float, ok: bool) -> bytes` uses orjson with keys `ts, tn, rg, un, lat, ok`, rounding `lat` to 0.1.
- `incidents.py`:
  - `@dataclass(frozen=True) Incident(kind: Literal["error_spike","volume_drop","latency_spike"], start_ms: int, end_ms: int, magnitude: float, region: str | None = None, key: str | None = None)`.
  - `Effects(volume_mult: float, error_add: float, latency_mult: float)`.
  - `effects(incidents, key: Key, t_ms: int) -> Effects` combines all active incidents matching the key or the region.
  - `demo_script(world: World, t0_ms: int) -> list[Incident]`: in M1 this is one `error_spike`, region `eu-central`, from t0 + 3 min to t0 + 13 min, magnitude +0.20.
- `gen_live.py`:
  - `class LiveGenerator(world, seed: int, incidents: list[Incident])` with `tick(second_ms: int) -> list[tuple[str, bytes]]`. It returns `(key, payload)` pairs **sorted by ts** for one second: per key, `n ~ Poisson(rate_rpm/60 × volume_mult)`, timestamps uniform in the second, latency log-normal, `ok ~ Bernoulli(1 − min(1, error_prob + error_add))`. It uses `numpy.random.default_rng(seed)` and draws in key order, so output is deterministic.
  - `main()` runs a CLI with `--bootstrap`, `--topic live.events`, `--world demo`, `--seed 1` and `--script demo|none`. It paces in real time, producing each second's batch with `confluent_kafka.Producer({"linger.ms": 50, "compression.type": "zstd"})`, keyed by key. It logs one summary line every 10 s.

- [ ] **Step 1: Write the failing tests**
  - `test_world.py`:
    - `build_world("demo", 1)` is deterministic: two builds compare equal.
    - It has 3 regions, keys are unique, and at least 2 units have `dedicated_for` set.
  - `test_traffic.py`:
    - `shape(3, 2)` = 1/6;
    - `shape(12, 2)` = 1;
    - `shape(12, 6)` = 0.3;
    - `shape(9.5, 0)` = 1.25;
    - `rate_rpm` in the tenant's local time: a tenant at +8 peaks at 04:00 UTC.
  - `test_events.py`: the decoded `encode(60000, "t01", "eu-central", "s1", 83.24, False)` equals `{"ts":60000,"tn":"t01","rg":"eu-central","un":"s1","lat":83.2,"ok":False}`.
  - `test_incidents.py`: a region incident affects only that region's keys and only inside `[start, end)`.
  - `test_gen_live.py`:
    - two generators with the same seed give identical `tick` output;
    - timestamps are non-decreasing and inside the second;
    - summed over 600 ticks at a fixed time, the count is within 10% of `Σ rate_rpm × 10`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-py` → import errors.
- [ ] **Step 3: Implement the modules.** Run `cd py && uv lock` to write `uv.lock`.
- [ ] **Step 4: Run the tests and linting and confirm they pass.** `make test-py && make lint-py` → all pass.
- [ ] **Step 5: Commit.** `git add py Makefile && git commit -m "Add synthetic world, traffic model and live event generator"`

### Task 8: Alert forwarder and pager sink

**Files:**
- Create: `py/src/tenant_baselines/forwarder.py`, `py/src/tenant_baselines/pager_sink.py`
- Test: `py/tests/test_forwarder.py`, `py/tests/test_pager_sink.py`

**Interfaces (produces):**
- `forwarder.py`:
  - `class ForwarderState(live_detector: str, live_setting: str, resend_s: float = 60, resolved_keep_s: float = 300, stale_s: float = 600)`.
    - `apply(event: dict, now_s: float) -> None`:
      - Ignores other detectors and settings.
      - Ignores a firing event whose `emitted_at_ms − event_time_ms > stale_s × 1000`, because it was decided during backfill.
      - A firing event adds the alert, idempotently by `alert_id`.
      - A resolved event sets `ends_at_ms = event_time_ms` and records `resolved_seen_s = now_s`. For an unknown id it does nothing.
    - `due(now_s: float) -> list[dict]` returns the Alertmanager API v2 entries for alerts never sent, or last sent at least `resend_s` ago. It first drops resolved alerts with `now_s − resolved_seen_s > resolved_keep_s`.
    - `mark_sent(alert_ids: list[str], now_s: float) -> None`.
  - `to_am_alert(a) -> dict`:
    - labels: `alertname` (`TenantAnomaly` when scope is tenant, else `RegionAnomaly`), plus `scope, region, tenant, unit, signal, detector, setting`, omitting empty values;
    - annotations: `summary`, `value` and `reference`;
    - `startsAt` in RFC 3339 UTC from `starts_at_ms`; `endsAt` only once resolved.
  - `main()`:
    - CLI flags: `--bootstrap`, `--topic live.alerts`, `--alertmanager http://alertmanager:9093`, `--live-detector global_static`, `--live-setting default`;
    - the consumer uses `group.id = "forwarder-" + uuid4()`, `auto.offset.reset = earliest` and `enable.auto.commit = false`, which rebuilds state from the topic on start;
    - every 5 s it POSTs `due()` to `/api/v2/alerts` with httpx (timeout 5 s). `mark_sent` runs only on a 2xx response, and errors are logged, never raised.
- `pager_sink.py`:
  - `make_server(host: str, port: int, log_path: str) -> ThreadingHTTPServer`. A POST to any path appends one JSON line `{"received_at": <iso>, "receiver": <path>, "body": <parsed JSON>}` to `log_path`, echoes it to stdout, and returns 200.
  - `main()` takes `--port 8099 --log /data/notifications.jsonl`.

- [ ] **Step 1: Write the failing tests**
  - `test_forwarder.py`, using event dicts shaped like `AlertEvent` JSON:
    - `test_fire_is_due_then_resent_after_60s`;
    - `test_duplicate_fire_is_idempotent`: applying the same event twice gives one entry from `due`;
    - `test_resolve_sets_endsAt_and_drops_after_300s`;
    - `test_stale_backfill_alert_is_skipped`: `emitted_at_ms − event_time_ms` = 3 h gives nothing due;
    - `test_other_detector_ignored`;
    - `test_failed_post_is_retried`: `main`'s send helper is given an httpx `MockTransport` returning 503; the alert is still due on the next tick and no exception is raised;
    - `test_labels_and_rfc3339`: `startsAt` equals `"1970-01-01T00:00:00Z"` for `starts_at_ms` 0.
  - `test_pager_sink.py`: start `make_server("127.0.0.1", 0, tmp)` in a thread, POST `{"x":1}` to `/tenant`, and check the log has one line with `receiver == "/tenant"` and `body == {"x":1}`.
- [ ] **Step 2: Run them and confirm they fail.** `make test-py`
- [ ] **Step 3: Implement both modules.**
- [ ] **Step 4: Run the tests and linting and confirm they pass.** `make test-py && make lint-py`
- [ ] **Step 5: Commit.** `git add py && git commit -m "Add Alertmanager forwarder and pager sink"`

### Task 9: Compose stack, topic creation, ClickHouse schema, Alertmanager and Grafana

**Files:**
- Create:
  - `docker-compose.yml`, `py/Dockerfile`, `infra/kafka/create-topics.sh`, `scripts/wait-ready.sh`
  - `infra/clickhouse/init/01-schema.sql`, `infra/alertmanager/alertmanager.yml`
  - `infra/grafana/provisioning/{datasources/clickhouse.yml,dashboards/provider.yml}`, `infra/grafana/dashboards/tenant-baselines.json`
- Modify: `Makefile`, adding `up` (`docker compose up -d --build && scripts/wait-ready.sh`), `down` and `reset` (`docker compose down -v`)

**Services** (memory limits in brackets; the total is 8.4 GB):

| Service | Memory | Setup |
|---|---|---|
| `kafka` | 1536m | `apache/kafka:4.4.0`, hostname `kafka`. Listeners `CONTROLLER://:29093`, `PLAINTEXT://:19092` (advertised `kafka:19092`) and `PLAINTEXT_HOST://:9092` (advertised `localhost:9092`), with the rest of the env from the official single-node plaintext example. Adds `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, `KAFKA_HEAP_OPTS=-Xms512m -Xmx1g`, `KAFKA_LOG_DIRS=/var/lib/kafka/data` on volume `kafka-data`. Healthcheck `/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092`. Port 9092. |
| `kafka-init` | 256m | Same image, `restart: "no"`, runs `create-topics.sh`. |
| `flink-data-init` | — | `flink:2.2.1-java17`, `user: root`, runs `chown -R 9999:9999 /flink-data` on volume `flink-data`, then exits. |
| `jobmanager` | 1024m | `flink:2.2.1-java17`, `command: jobmanager`, port 8081. Mounts `flink-data:/flink-data`, `./flink-job/target:/opt/tb-jars:ro`, `./infra/flink:/opt/tb-config:ro` and `./scripts:/opt/tb-scripts:ro`. Depends on `flink-data-init` completing. |
| `taskmanager` | 3072m | `flink:2.2.1-java17`, `command: taskmanager`, mounts `flink-data:/flink-data`. |
| `clickhouse` | 2048m | `clickhouse/clickhouse-server:26.8.36.3`, env `CLICKHOUSE_DB=tb`, `CLICKHOUSE_USER=tb`, `CLICKHOUSE_PASSWORD=tb-local`, `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`. Volume `ch-data:/var/lib/clickhouse`, `./infra/clickhouse/init:/docker-entrypoint-initdb.d:ro`, ulimit `nofile` 262144, port 8123. Healthcheck `wget -qO- http://localhost:8123/ping`. Depends on `kafka-init` completing successfully. |
| `alertmanager` | 128m | `prom/alertmanager:v0.34.2`, config mounted read-only, port 9093. |
| `pager-sink` | 256m | Built from `py/Dockerfile`, runs `python -m tenant_baselines.pager_sink --port 8099 --log /data/notifications.jsonl` on volume `pager-data`. |
| `forwarder` | 256m | `py` image, runs `python -m tenant_baselines.forwarder --bootstrap kafka:19092 --topic live.alerts --alertmanager http://alertmanager:9093 --live-detector global_static --live-setting default`, with `restart: unless-stopped`. |
| `generator` | 512m | `py` image, `profiles: ["demo"]`, runs `python -m tenant_baselines.gen_live --bootstrap kafka:19092 --topic live.events --world demo --seed 1 --script demo`. |
| `grafana` | 512m | `grafana/grafana:13.2.5`, env `GF_PLUGINS_PREINSTALL_SYNC=grafana-clickhouse-datasource@4.23.4`, `GF_AUTH_ANONYMOUS_ENABLED=true`, `GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer`. Mounts the provisioning and dashboards directories read-only, plus volume `grafana-data`. Port 3000. |

**Flink config** (`FLINK_PROPERTIES`, the same in the JobManager and TaskManager):
```
jobmanager.rpc.address: jobmanager
state.backend.type: rocksdb
execution.checkpointing.incremental: true
execution.checkpointing.dir: file:///flink-data/checkpoints
execution.checkpointing.interval: 30 s
restart-strategy.type: fixed-delay
restart-strategy.fixed-delay.attempts: 10
restart-strategy.fixed-delay.delay: 10 s
jobmanager.memory.process.size: 1024m
taskmanager.memory.process.size: 3072m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 2
```

**Topics** (`create-topics.sh`): idempotent (`--if-not-exists`), `compression.type=zstd` on every topic. For each `ns` in `live`, `eval` and `bench`:
- `<ns>.events`: 8 partitions, `retention.ms=86400000`;
- `<ns>.aggregates`: 4 partitions;
- `<ns>.scores`: 4 partitions;
- `<ns>.alerts`: 1 partition;
- `<ns>.unit-status`: 1 partition, `cleanup.policy=compact`.

**`py/Dockerfile`:**
- `FROM python:3.12-slim`;
- `COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /uvx /bin/`;
- copy `pyproject.toml`, `uv.lock` and `src/`;
- `uv sync --frozen --no-dev`;
- `ENV PATH=/app/.venv/bin:$PATH`.

**`01-schema.sql`** (exact table set):
- **Kafka-engine tables:**
  - `tb.kafka_aggregates` declares every field of `MinuteAgg`'s JSON, with `minute_ms Int64` and `p95_latency_ms Nullable(Float64)`. It reads topics `live.aggregates,eval.aggregates,bench.aggregates`, group `ch-aggregates`.
  - `tb.kafka_alerts` declares every field of `AlertEvent`'s JSON, with `*_ms Int64` and `reason Nullable(String)`. It reads `live.alerts,eval.alerts,bench.alerts`, group `ch-alerts`.
  - Both use `kafka_format='JSONEachRow'`, `kafka_num_consumers=1` and `kafka_handle_error_mode='stream'`.
- **`tb.aggregates`:** `ReplacingMergeTree`, `ORDER BY (ns, run_id, key, minute)`. Columns: `ns LowCardinality(String)`, `minute DateTime64(3, 'UTC')`, and the rest as LowCardinality strings, UInt64 counts and nullable p95.
- **`tb.alert_log`:** `ReplacingMergeTree`, `ORDER BY (ns, run_id, alert_id, state)`. It stores `starts_at`, `event_time` and `emitted_at` as `DateTime64(3, 'UTC')`.
- **`tb.ingest_errors`:** `MergeTree ORDER BY at` with columns `at DateTime64(3) DEFAULT now64(3), topic String, error String, raw String`.
- **Materialized views:**
  - `tb.mv_aggregates` and `tb.mv_alerts` select `splitByChar('.', _topic)[1] AS ns`, convert every `*_ms` with `fromUnixTimestamp64Milli(x, 'UTC')`, and filter `WHERE length(_error) = 0`.
  - `tb.mv_aggregates_errors` and `tb.mv_alerts_errors` insert `_topic`, `_error` and `_raw_message` into `tb.ingest_errors` `WHERE length(_error) > 0`.

**`alertmanager.yml`:**
- root route: receiver `tenant-pager`, `group_by: [alertname, region, signal]`, `group_wait: 30s`, `group_interval: 5m`, `repeat_interval: 4h`;
- child routes: `matchers: [scope="region"]` → `regional-pager`, and `[scope="tenant"]` → `tenant-pager`;
- receivers: `webhook_configs` to `http://pager-sink:8099/regional` and `/tenant`, with `send_resolved: true`;
- `inhibit_rules`: `source_matchers: [scope="region"]`, `target_matchers: [scope="tenant"]`, `equal: [region, signal]`.

**Grafana:**
- **Datasource:** type `grafana-clickhouse-datasource`, uid `clickhouse`, with `jsonData: {host: clickhouse, port: 9000, protocol: native, username: tb, defaultDatabase: tb}` and `secureJsonData.password: tb-local`.
- **Dashboard** uid `tenant-baselines`: a template variable `key` from `SELECT DISTINCT key FROM tb.aggregates WHERE ns='live' ORDER BY key`, and two panels:
  1. a time series for `$key`: error rate (`errors/count`), p95 latency and the 5-minute volume sum, from `tb.aggregates FINAL`, with the demo static fire levels as panel thresholds;
  2. a table of the latest 100 rows of `tb.alert_log FINAL WHERE ns='live'`.

**`wait-ready.sh`** polls, with a 180 s timeout per check and exit 1 with the name of what failed:
- Kafka health;
- `kafka-init` has exited 0;
- ClickHouse `/ping`;
- Flink `GET :8081/overview` reports `taskmanagers ≥ 1`;
- Alertmanager `/-/ready`;
- Grafana `/api/health`.

- [ ] **Step 1: Validate the configs before starting**
  Run: `docker compose config -q`
  Expected: no output, exit 0.
  Run: `docker run --rm -v $PWD/infra/alertmanager:/c prom/alertmanager:v0.34.2 amtool check-config /c/alertmanager.yml`
  Expected: `SUCCESS`.
- [ ] **Step 2: Bring the stack up.** `make up` → it prints `ready` for every service.
- [ ] **Step 3: Verify topics and schema**
  Run: `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | wc -l`
  Expected: 15.
  Run: `curl -s -u tb:tb-local 'http://localhost:8123/?query=SHOW%20TABLES%20FROM%20tb'`
  Expected: `aggregates, alert_log, ingest_errors, kafka_aggregates, kafka_alerts` and the four views.
  Run: `curl -s localhost:3000/api/dashboards/uid/tenant-baselines | python3 -c "import sys,json; print(json.load(sys.stdin)['dashboard']['title'])"`
  Expected: the dashboard title.
- [ ] **Step 4: Check memory.** `docker stats --no-stream` shows every container under its limit and none restarting.
- [ ] **Step 5: Commit.** `git add -A && git commit -m "Add compose stack, topics, ClickHouse schema, Alertmanager and Grafana"`

### Task 10: `make demo`, smoke test, CI and README stub

**Files:**
- Create: `scripts/submit-job.sh`, `scripts/smoke.sh`, `.github/workflows/ci.yml`, `README.md`
- Modify: `Makefile`, adding `submit`, `demo`, `smoke`, `test` (`test-java` + `test-py` + `lint-py`), `ci` and `logs`

**Behaviour:**
- **`submit-job.sh`** runs inside the jobmanager via `docker compose exec`:
  - it cancels any RUNNING job named `tenant-baselines-live` (from the `GET /jobs/overview` REST call, then `flink cancel <id>`);
  - then runs `flink run -d /opt/tb-jars/tenant-baselines-job.jar --bootstrap kafka:19092 --ns live --run-id live --mode live --thresholds /opt/tb-config/thresholds-demo.json`.
- **`make demo`:**
  - runs `jar`, `up` and `submit`, then `docker compose --profile demo up -d generator`;
  - prints `Grafana http://localhost:3000/d/tenant-baselines`, `Alertmanager http://localhost:9093`, `Flink http://localhost:8081`;
  - also prints `In about 3 minutes eu-central gets an error spike for 10 minutes. Alerts appear in Alertmanager about 4 minutes after it starts.`
- **`make smoke`** runs `make demo`, then `smoke.sh`. It polls every 15 s with a 12-minute limit, printing `PASS <check>` or `FAIL <check>` and exiting non-zero on any failure:
  1. `SELECT count() FROM tb.aggregates FINAL WHERE ns='live'` > 0;
  2. `tb.alert_log FINAL` has a `firing` row with `signal='error_rate' AND region='eu-central'`;
  3. `GET :9093/api/v2/alerts?filter=alertname%3D%22TenantAnomaly%22` is non-empty;
  4. `docker compose exec pager-sink wc -l /data/notifications.jsonl` ≥ 1;
  5. `SELECT count() FROM tb.ingest_errors` = 0;
  6. resubmit the job with `scripts/submit-job.sh`, wait 60 s, then check that `SELECT count() - uniqExact(key, minute) FROM tb.aggregates FINAL WHERE ns='live'` = 0 and `SELECT count() - uniqExact(alert_id, state) FROM tb.alert_log FINAL WHERE ns='live'` = 0.
- **`ci.yml`:** on `push` and `pull_request`, two jobs:
  - **`java`:** `actions/checkout`, `actions/setup-java` (`distribution: temurin`, `java-version: 17`, `cache: maven`), then `mvn -B verify` in `flink-job`;
  - **`python`:** `actions/checkout`, `astral-sh/setup-uv`, then `uv sync`, `uv run ruff check .`, `uv run ruff format --check .` and `uv run pytest -q` in `py`.
  - Pin each action to the major of its latest release, found with `gh api repos/<owner>/<repo>/releases/latest --jq .tag_name`.
- **`make ci`:** runs the `java` job's command in `maven:3.10.0-eclipse-temurin-17-noble`, and the `python` job's commands in `python:3.12-slim` with uv copied in. These are the same commands as the workflow.
- **`README.md`** (stub, about 25 lines):
  - one-paragraph purpose; a "Work in progress: milestone 1 of 6" line; a `Synthetic data only` note;
  - prerequisites: Docker with ≥ 8 CPUs and 10 GB, `make` and `uv`;
  - quickstart: `docker compose up`, then `make demo`;
  - links to the spec.
  - Plain words; no claims of results yet.

- [ ] **Step 1: Run the full test suite.** `make test` → Java and Python 0 failures, ruff clean.
- [ ] **Step 2: Run CI locally.** `make ci` → both jobs exit 0.
- [ ] **Step 3: Run the end-to-end smoke test.** `make smoke` → `PASS` for all 6 checks. On a FAIL, find the cause before changing code.
- [ ] **Step 4: Check by hand in the browser.** Open the Grafana dashboard and confirm both panels show data for an `eu-central` key. In Alertmanager, confirm the `TenantAnomaly` alerts grouped by region and signal.
- [ ] **Step 5: Do the repo hygiene check.**
  Run: `git grep -n -i -E 'spearhead|leverag|synerg|cutting-edge|results-driven|passionate'`
  Expected: no matches outside `docs/process/`.
- [ ] **Step 6: Commit and stop.**
  `git add -A && git commit -m "Milestone 1: end to end with static thresholds"`
  Then stop for the owner's review. Report what works and the smoke output, and say plainly that the GitHub Actions workflow hasn't run on GitHub.
