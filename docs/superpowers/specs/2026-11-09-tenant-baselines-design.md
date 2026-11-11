# tenant-baselines: design

Status: v2, 2026-11-09. Revised after two independent reviews: one of systems correctness, one of detection and eval method.
Owner: Sanchay Pahalwani. Builder: Claude Code, working in this folder.

Planning material. It is written so it can be published, but the owner decides before the repo goes public whether `docs/superpowers/` stays in it.

## 1. Intent

`tenant-baselines` is a small reference pipeline for multi-tenant anomaly alerting on service telemetry. It runs on one laptop: Kafka → Flink → Kafka topics → ClickHouse and Grafana, with alerts sent through Prometheus Alertmanager.

**The claim it tests.** Fixed thresholds fit badly when traffic follows customers' working hours. Set high enough to stay quiet at peak, a threshold misses trouble at night; set low enough to catch it, the threshold pages every afternoon. A baseline per key, per hour of the week, should catch more and page less.

**How it tests it.** The repo measures the claim on labelled synthetic replays and publishes the numbers, the scripts and the commits that produced them.

**What it can and can't show.** The replay's traffic follows working hours *because the generator says so*. So the eval tests whether this implementation delivers the benefit when that assumption holds, and how the benefit changes as seasonality weakens. It is not evidence about any real fleet, and the docs say so.

The owner has to be able to explain and defend every choice. Every choice is written down with its reason: here for building, and in `docs/design.md` for readers.

## 2. Goals, non-goals, anti-goals

**Goals (v1)**
- `docker compose up` and then `make demo` work from a clean clone on a laptop, with live alerts visible in Alertmanager and Grafana.
- A labelled synthetic eval that scores seasonal baselines against two kinds of static threshold:
  - 5 pre-registered seeds;
  - a 3-point sweep of seasonality strength;
  - results reported as counts with 95% intervals;
  - `docs/results.md` holds the tables, the commit hash and the command that produced them.
- `docs/performance.md`, produced by a script, with the exact hardware and Docker resources:
  - sustained throughput;
  - pipeline delay and time-to-page, at p50 and p95;
  - key cardinality;
  - state size.
- `README.md` and `docs/design.md` explain every choice in plain language.
- Tests pass. The GitHub Actions workflow exists, and `make ci` runs the same commands locally (§7).

**Non-goals**
- Production hardening: TLS, auth, multi-broker Kafka, high-availability JobManager, schema registry.
- A second OLAP store. ClickHouse only; `docs/design.md` explains why.
- Acting on unit status, i.e. actually moving traffic (§4.8).
- Holiday calendars, DST, change-point detection.
- Stretch items, dropped first: OTLP ingest through an OpenTelemetry Collector; the Flink Kubernetes Operator on a local cluster; a generated "what changed" note per alert.

**Anti-goals: the build has failed if any of these happens, even when everything runs**
- A number in the README or docs that no script in the repo produces.
- Arithmetic presented as measurement. Estimates are labelled as estimates.
- Synthetic data not labelled as synthetic, or wording that implies production use, or that the result transfers to real traffic.
- Any employer, client, internal system or company name anywhere in the repo.
- A setting tuned on a test week, or on an official seed.
- A static opponent a careful reader could call a strawman (§4.6).
- Marketing vocabulary. Prose stays plain and specific.

## 3. Constraints

- **Pinned versions** (Maven Central and Docker Hub checked 2026-11-09):
  - Flink **2.2.1** (`flink:2.2.1-java17`) with `flink-connector-kafka` **5.0.0-2.2**. Flink 2.3.0 is out, but the newest Kafka connector targets 2.2.
  - flink-connector-datagen **2.2.1** for the live tick source; it ships inside the Flink distribution. *(Added 2026-11-11.)*
  - Java **17**, Flink 2.x's recommended runtime. Maven build.
  - Kafka **4.4.0** (`apache/kafka`), a single node in KRaft combined mode, with `auto.create.topics.enable=false`.
  - ClickHouse **26.8**, Alertmanager **v0.34.2**, Grafana **13.2.5** with the official ClickHouse data-source plugin. Full patch tags and the plugin version are pinned at build time.
  - Python **3.12** with `uv`.
- Before each milestone, re-check the APIs it uses with the `ctx7` docs CLI, not from memory.
- **No local JDK is needed.** The job builds in a `maven` + Temurin 17 container through `make`.
- **Hardware:** Apple M3 Pro, 12 cores, 18 GB RAM. Images must run on arm64.
- **Docker VM** *(owner's call)*: Colima at **8 CPUs / 10 GB**, set before the first measurement. Every published number records it.
  - Containers get explicit memory limits that fit inside 10 GB.
  - Kafka, ClickHouse, RocksDB working directories and checkpoints use **named Docker volumes**, not bind mounts. File sharing to macOS would distort the I/O-heavy numbers.
- **License:** Apache-2.0.
- **Git:** local only. Commit at the end of each milestone, then **stop for the owner's review** before starting the next. Never create a GitHub repo, add a remote or push.

## 4. How it works

### 4.1 Pipeline

```
event generator (Python) ─▶ <ns>.events ─┐
                                          ├─ Flink job (Java)
aggregate generator (Python, eval) ─▶ <run>.aggregates-in ─┘
   1a. events mode:     parse → watermarks → MinuteAggregator   (keyed by key)
   1b. aggregates mode: read pre-built minute aggregates (eval only)
   2.  PopulationBaselines   parallelism 1 → broadcast region/global fallback baselines
   3.  Scorer                keyed by key  → scores (live config only), tenant alert candidates
   4.  RegionGrouper         keyed by region → alerts, unit-status
        ▼
  Kafka: <ns>.aggregates, <ns>.scores, <ns>.alerts, <ns>.unit-status
        ├─▶ ClickHouse (Kafka table engine + materialized views) ─▶ Grafana
        └─▶ live.alerts ─▶ forwarder (Python) ─▶ Alertmanager ─▶ pager-sink (webhook log)
```

- **Namespaces:** `<ns>` is `live`, `eval` or `bench`. ClickHouse uses one database. Each row carries an `ns` (taken from the source topic) and a `run_id`.
- **Key:** `tenant|region|unit`. A unit is either *shared* (several tenants) or *dedicated* (one tenant). On a dedicated unit the key's baseline is in effect that tenant's own baseline, so one tenant's trouble shows even when its region looks normal.
- **The two input modes share everything from step 2 on.** The aggregates mode lets the eval run many seeds in minutes (§5). The events mode is what the demo and the benchmark use. The equivalence check in §5.7 proves the two modes give the same alerts.

### 4.2 Events, watermarks and per-minute aggregates *(builder's calls)*

**Events.** One event is one request: `ts` (event time, ms), `tenant`, `region`, `unit`, `latency_ms`, `ok`. JSON with short field names, zstd on the topic.

**Watermarks.**
- **Replay** (events read from the past): pure event time. Bounded out-of-orderness of 20 s, idle partitions marked idle after 30 s, and Kafka-source watermark alignment, so no partition runs far ahead. The generator writes **in time order per partition**. This is a hard requirement: out-of-order writes would be silently dropped as late.
- **Live:** the events source uses plain event time per partition: bounded out-of-orderness of 20 s, and a partition is marked idle after 60 s without an event. A second source emits one tick a second, stamped with the wall clock, with the same 20 s bound. The two streams are unioned and the ticks are filtered out before `MinuteAggregator`, so its watermark is the lower of the two.
  - While the source is behind (backfill, or a backlog after a restore), that is pure event time.
  - Once it has caught up, the watermark is the wall clock − 20 s, held back by the newest event of the slowest partition that is not idle: about a second more when every partition carries traffic, at most 60 s more when one is sparse (21 s to 80 s behind the wall clock in all).
  - When every producer goes silent, the partitions go idle after 60 s and the ticks carry the wall clock − 20 s. This also holds after a restore or a failover during the silence. That matters, because a total outage is exactly when minutes must keep closing.
  - After a start, a restore or a failover, no minute closes until every partition has delivered an event or has been silent for 60 s. A partition that has been silent for 60 s is left out until its next event; events written to it later with timestamps behind the watermark are dropped as late. `make demo` submits the job only after every partition has data.

  *(Amended 2026-11-11 by the owner: this replaces the custom generator that latched to the wall clock. That one could not advance after a restore during a total outage, and it could hand over to the wall clock while a partition still had unread events.)*

**Late events.** Events behind the watermark go to a side output that is counted and exposed as a metric. They are not re-aggregated.

**MinuteAggregator.** A `KeyedProcessFunction`, not a window, because silent keys must still produce minutes.
- Each finished minute emits `count`, `errors` and `p95_latency_ms`. The p95 comes from a fixed log-bucket histogram with about 3% precision, and the Python aggregate generator uses the same bucket scheme.
- **A silent key keeps emitting zero-count minutes.** Silence is the strongest volume-down signal, and windows emit nothing for an empty minute.
- **End of input:** at the final `Long.MAX_VALUE` watermark, the zero-fill timer does **not** re-register. Without this, a bounded replay would loop, emitting zero minutes until every key expired, and page on all of them. A harness test covers it.

**Key expiry.** After 7 days of event time with no events, an event-time timer clears the key's state in each operator, and the Scorer emits a resolve (reason `expired`) for anything still firing. Flink's built-in state TTL counts processing time only, so it means nothing in a replay.

### 4.3 Seasonal baseline *(owner's calls, except where marked)*

**Buckets and statistic.**
- Per key and per signal there are 168 hour-of-week buckets, in UTC.
- Each bucket keeps one-minute values from the **last 4 weeks**, not counting the current week: up to 240 values in a ring of 5 weekly slots.
- Baseline = **median**; spread = `1.4826 × MAD`.
- Robust statistics are the point: an outage last week barely moves the median.
- Every minute feeds the baseline, including minutes while an alert is firing.

**Signals and scores** *(builder's calls on the floors and windows)*. A score is the distance from baseline in units of spread, in the bad direction only.
- **Error rate up.**
  - Spread = `max(1.4826·MAD, √(p₀(1−p₀)/n))`, with `p₀ = max(median, 0.5%)` and `n` the minute's request count.
  - The second term is binomial sampling noise. Without it, MAD is 0 on most keys and a single error looks infinite.
  - At n = 37, one error scores about 2.3 and two score about 4.6.
- **Latency up** (p95).
  - Spread = `max(1.4826·MAD, 5% of median)`.
  - MAD already includes p95 sampling noise, because history minutes have similar counts.
- **Volume down**, scored on the **trailing 5-minute sum**:
  - expected = the sum of those 5 minutes' bucket medians;
  - spread = `max(√5 · 1.4826·MAD, √expected)`.
  - Per-minute volume can't see silence on small keys: silence scores √median, which only reaches 4 at 16 requests per minute. The 5-minute sum reaches 4 at about 3.2 requests per minute.

**Zero-traffic rule** (every key, warm or cold). Fires when there are zero requests for 5 minutes in a row and at least 15 were expected. The chance of that happening by luck is about e⁻¹⁵. Expected comes from the baseline when there is one, and from the trailing 30-minute average otherwise.

**Minimum evidence.** Error rate and latency aren't scored in a minute with fewer than 20 requests. This isn't tuned; it follows from the sampling noise. Below 20, one error is 5+ points and the p95 is roughly the maximum.
- An unscored minute **pauses** the fire and resolve streaks; it neither extends nor resets them.

**Caching.** A bucket's median and spread can't change during its hour, since the current week isn't in its baseline. They are computed once when the hour starts.

**State size estimate** (an estimate; §6 measures the real figure): 3 signals × 168 buckets × 5 slots × 60 values × 4 B ≈ 600 KB per key.

**Known limits** (recorded in `design.md`):
- Hour buckets blur within-hour ramps: spread is about 0.37 × the ramp range, so ramp hours are less sensitive (but don't false-fire).
- UTC buckets would shift an hour at a DST change. The synthetic world uses fixed offsets.
- A lasting problem is absorbed into the baseline in about 2 weeks, and its alert then resolves silently.

### 4.4 Cold start *(owner's call)*

- A key uses fallback in a bucket while it has fewer than **2 weeks** of data there. "A week of data" means at least 30 scored minutes in that bucket that week.
- **Error rate and latency** fall back to the region's baseline, then the global one.
- **Volume-down waits** for the key's own 2 weeks. The zero-traffic rule covers it meanwhile.
- **How fallback baselines are built** *(builder's call)*: `PopulationBaselines` runs at parallelism 1 and keeps a log-bucket histogram of all keys' minute values, per region and globally, per hour-of-week bucket, per week. Median and MAD are read from the histogram. When a bucket's hour ends, the operator broadcasts that bucket's baseline for next week. Updates arrive a week before they're used, so the unordered broadcast input can't race.

### 4.5 Fire and resolve: shared by every detector *(owner's call)*

- **Fire** after **3 scored minutes in a row** at or past the fire level.
- **Resolve** after **5 scored minutes in a row** inside the resolve level.
- Seasonal default: fire ≥ 4, resolve < 2.
- **Shared by all three detector families:** this state machine, minimum evidence, the pause rule, the zero-traffic rule and regional grouping. Only the reference differs: a fixed threshold or the hour-of-week baseline. So the comparison isolates the effect of the baseline.
- **Alert IDs are deterministic:** a hash of detector, setting, scope, key or region, signal and start minute. Re-fires of the same key and signal are counted (§5.5).

### 4.6 Static opponents *(owner's call: both; builder's calls on the details)*

- **Global static:** one threshold per signal, the same for every key.
- **Per-key static:** each key's own fixed threshold.
- **Same signal definitions as seasonal**, including the 5-minute volume sum.
- **Fire and resolve levels:** fire when the value passes `T_fire` for 3 minutes in a row. Resolve after 5 minutes back inside `T_resolve`, which is halfway between the calibration median and `T_fire`. That gives static the same hysteresis seasonal has.
- **Thresholds** are percentiles of the calibration minutes, computed in Python from the same minute aggregates the job sees. They are written to a thresholds file with `valid_from` and `valid_to`, and the job loads it at start.
  - Doing this outside the job avoids a streaming calibration operator and a broadcast race.
  - It's also how an operator sets a static threshold: look at the history once, then fix it.
- **Calibration, chosen to favour the opponent:**
  - The week-4 thresholds are fitted on weeks 1–3.
  - The week-5 thresholds are refitted on weeks 1–4, keeping the percentile chosen on week 4.
  - The minutes of every labelled scenario are **masked out** of the fit: the should-page incidents, and the launch surges and holiday dips too. Only the Monday-morning peak, which is part of every week, stays in. Seasonal gets no such help, since its design feeds every minute. *(Amended 2026-11-12 by the owner: the mask covers every labelled scenario except the Monday peak, where this said "labelled incident minutes". A surge left in the fit would lift the high-percentile latency thresholds to surge level.)*
- **A key with no calibration data** (onboarded later) uses the global threshold.

**Settings grids: 4 per family, fixed here** *(builder's call)*:

| Family | Settings |
|---|---|
| seasonal | fire k ∈ {3, 4, 5, 6}, resolve at k/2 |
| per-key static | percentile ∈ {99, 99.9, 99.97, 99.99} for up signals; mirrored {1, 0.1, 0.03, 0.01} for volume |
| global static | same percentiles as per-key |

### 4.7 Regional grouping *(owner's call; builder's calls on the edge cases)*

- **Hold:** `RegionGrouper` holds each tenant alert candidate for **2 minutes of event time**.
- **Promote:** when the hold ends, it checks the share of the region's **active tenants** with an alert on the same signal. An active tenant has a key in the region averaging at least 20 requests per minute over the last hour, and a region needs at least 3 of them. At **≥ 50%** it fires **one regional alert** and absorbs the held tenant alerts (logged as `absorbed`). Otherwise the tenant alert fires.
- **Regional resolve:** the share stays under 25% for 5 minutes.
- **On regional resolve,** absorbed tenant alerts that are still firing go through a fresh hold, so nothing goes silent.
- **Alerts already sent stay sent.** A tenant alert sent before the regional alert formed has already paged. The eval counts it; in the demo an Alertmanager inhibit rule mutes its repeats.
- **Night outages:** at night few tenants may be active, so a regional outage can turn into several tenant pages. The eval reports, per scenario, whether a regional alert formed and how many pages there were.

### 4.8 Unit status *(owner's call: publish only)*

- `RegionGrouper` writes `drain` and `undrain` events to a compacted topic, `<ns>.unit-status`, shown in Grafana. Nothing acts on them.
- **Drain rule** *(builder's call)*: a unit is drained while its keys show an error-rate or latency alert that isn't absorbed into a regional alert. On a dedicated unit, one tenant's alert is enough; on a shared unit, at least 2 tenants must alert on the same signal.
- **No drain on volume-down,** because the cause of a traffic drop is often upstream of the unit. Never drain during a regional alert either: draining a whole region moves traffic nowhere.

### 4.9 Sinks, ClickHouse and idempotency *(owner's call: ClickHouse pulls from Kafka)*

- **The job writes only to Kafka, at least once.** Exactly-once sinks release output only on checkpoint commit, which would add up to a checkpoint interval to every alert.
- **Duplicates are removed downstream** by deterministic keys, in `ReplacingMergeTree` tables fed by Kafka-engine tables and materialized views. Dashboards query with `FINAL` or `argMax`.

| Table | Deduplication key | Contents |
|---|---|---|
| `aggregates` | `(ns, run_id, key, minute)` | per-minute aggregates |
| `scores` | `(ns, run_id, key, signal, minute)` | the live seasonal setting only; every other setting emits alerts only, which keeps volume down |
| `alert_log` | `(ns, run_id, alert_id, state)` | fire, resolve and absorbed events |
| `unit_status` | `(ns, run_id, unit, at)` | drain and undrain events |

- **Topics are never deleted.** Each eval run gets its own input topic and a `run_id` on every record, so the Kafka-engine tables never point at a missing topic.
- **Checkpointing:** every 30 s, RocksDB state backend, incremental checkpoints, named volume, fixed-delay restart. Exactly-once *state* with at-least-once *output*; `design.md` explains the difference.

### 4.10 Alert delivery and the demo *(builder's calls)*

**Forwarder (Python).**
- Reads `live.alerts` for the live setting only (seasonal, k = 4).
- Re-sends each firing alert to Alertmanager's `/api/v2/alerts` every 60 s. On resolve it sends `endsAt` and keeps re-sending for 5 minutes.
- Skips an alert that was decided more than 10 minutes after its event time (`emitted_at − event_time > 10 min`), so backfill history never pages. An incident still firing when the forwarder restarts keeps paging. *(Amended 2026-11-10 by the owner, to match the milestone 1 build.)*
- On start, it rebuilds its firing set by reading the topic from the beginning.

**Alertmanager.**
- Groups by `alertname`, `region` and `signal`: `group_wait` 30 s, `group_interval` 5 min, `repeat_interval` 4 h.
- Routes regional and tenant alerts to separate receivers, both webhooks to `pager-sink`, which logs each notification.
- An inhibit rule mutes tenant alerts while a regional alert with the same region and signal is firing.

**Grafana.** Two provisioned panels until milestone 6: one key's three signals with the baseline band, and the alert log. Milestone 6 adds a score heatmap by region, notifications and unit status.

**`make demo`.**
- Uses a small world of about 20 keys.
- The generator runs **one continuous timeline:** it starts 3 weeks back (enough to warm the baselines), produces at full speed until it reaches the wall clock, then slows to real time. The script shows progress and should warm up in about 3 minutes.
- With no seam between history and live traffic, there's no fake volume drop at the switch.
- The demo's static thresholds come from the same script run on the demo history.
- After catch-up it injects a scripted sequence: a dedicated-unit tenant drop; an error burst on one tenant's dedicated unit, which pages and drains the unit; then an error spike across that tenant's region, which forms one regional alert. It prints the URLs and what to watch for. The demo world has three small tenants with a flat traffic profile in that region, so a regional alert can form at any hour; without them no region has three active tenants on nights and weekends. *(Amended 2026-11-11, builder's call, for milestone 3.)*

## 5. Eval

### 5.1 Input

- **The detection eval feeds generated per-minute aggregates into the same Flink job** (aggregates mode). The generator simulates every request with numpy (Poisson counts, binomial errors, per-request latencies), then aggregates them with the same p95 bucket scheme as the job. No per-event Kafka traffic is needed.
- A run takes minutes, which is what makes 5 seeds and a sweep affordable.
- The event path is covered by tests, the demo, the benchmark and the equivalence check in §5.7.

### 5.2 Traffic model (synthetic; main setting)

- **World:** 3 regions, 24 tenants, about 60 keys. Tenants run on 2 shared units per region, or on a dedicated unit (6 tenants).
- **Time zones:** fixed UTC offsets of −5, +1 and +8 for the three regions. About 20% of tenants are served from a region outside their own time zone.
- **Key size:** weekday peak requests per minute are log-normal (median 150, σ = 0.8). So some keys are small enough to fall under minimum evidence at night, and that behaviour is visible in the results.
- **Seasonality:**
  - a working-hours curve in each tenant's local time, with **day/night amplitude 6:1** in the main setting;
  - weekends at 30% of weekdays;
  - a Monday 08:00–11:00 local bump of +25%.
- **Nuisance the baseline doesn't model**, so the median isn't an oracle:
  - a per-tenant, per-day log-normal level factor (σ = 0.15);
  - a slow per-tenant trend, uniform in ±3% per week;
  - unlabelled harmless bursts: about 1 per tenant per day, +80% volume for 5 minutes.
- **Errors and latency:**
  - base error rate 0.3%, plus a small term that grows with load;
  - latency log-normal around a unit-specific median of 60–120 ms, multiplied by `1 + 0.6·(load/peak)²`.
  - Load drives latency through this fixed curve, so surges get whatever latency rise the curve gives. That isn't a free parameter.

### 5.3 Weeks, seeds and the sweep

**Weeks.**
- **Weeks 1–3: fit.** A few scattered labelled incidents show the baseline survives incidents in its own history.
- **Week 4: tune.** Each family picks its setting here (§5.6).
- **Week 5: test.**

**Seeds** *(owner's call)*.
- All development, including every builder-tuned setting, uses **dev seeds**.
- Settings freeze in a commit. Then **5 official seeds**, listed in that commit before any of them runs, each run **once**.
- Each seed does its own week-4 selection. Results report the median and range across seeds, plus pooled counts.
- Caveat: dev and official seeds come from the same generator, so official seeds test fresh draws, not a different world.

**Sweep.** Day/night amplitude **1.5:1, 6:1 (main) and 12:1**, all 5 official seeds at each point. If the gap between seasonal and static shrinks as amplitude falls, that supports the mechanism the claim rests on. One operating point can't show it.

**Exit rule** *(owner's call)*.
- Whatever the official runs show is published, including a result where seasonal doesn't beat per-key static. In that case the README says so plainly.
- **Bug fixes:** a bug found afterwards may be fixed and re-run on the same seeds. A *bug* means the code disagrees with this document; anything else is a setting change and is not allowed.
- `results.md` keeps a run log: commit, date, what changed and why, and the old and new headline numbers.

### 5.4 Scenarios: pre-registered here, before any detector code exists

Each of week 4 and week 5, in every seed, gets the scenarios below. Within each scenario's eligibility rule, the seeded RNG picks the keys and times. Magnitudes are fixed. Any change to this table after detector code exists counts as a setting change.

| Scenario | Count / week | Magnitude | Eligibility | Expected signal | Should page |
|---|---|---|---|---|---|
| Regional outage | 1 | error rate +15 points and latency ×1.5, all keys in one region, 40 min | any hour; the hour is reported | error rate | yes, ideally as one regional alert |
| Dedicated-unit tenant drop, working hours | 1 | volume ×0.3, 60 min | tenant's local 09:00–17:00 weekday | volume | yes |
| Dedicated-unit tenant drop, night or weekend | 1 | volume ×0.3, 60 min | expected 5-minute volume ≥ 15 | volume | yes |
| Slow latency creep, one unit | 2 | latency 1.0× → 2.5× linearly over 6 h, hold 1 h | unit's keys ≥ 20 requests per minute for most of the window | latency | yes |
| Off-hours latency spike, one key | 2 | latency ×2.5, 30 min | 00:00–06:00 local, key volume ≥ 25 requests per minute then | latency | yes |
| Tenant error burst, shared unit | 2 | error rate +8 points, 20 min | key ≥ 30 requests per minute | error rate | yes |
| New-tenant error burst (cold start) | 1 | error rate +8 points, 20 min | the tenant onboarded at the start of week 4 | error rate | yes |
| Launch surge, one tenant | 1 | volume ×3 for 3 h; latency from the load curve | weekday working hours | none | no |
| Regional holiday dip | 1 | one region's tenants at 35% volume for one local day | weekday | none | no; hour-of-week baselines are expected to fail here |
| Monday-morning peak | built into every week | (traffic model) | all keys, Monday 08:00–11:00 local | none | no |

That gives **10 should-page scenarios per week**: 50 per family across the 5 official seeds at the main setting.

### 5.5 Scoring *(builder's calls)*

- **Page:** a fire event after regional grouping, tenant or regional. Resolves and absorbed alerts aren't pages. Pages count only if their start minute falls inside the scored week.
- **Pages come from `eval.alerts` in event time, not from Alertmanager.** Alertmanager's grouping timers run on the wall clock and can't follow a replay.
- **Detected:** a page fires between scenario start and end + 10 minutes, on an affected key or as a regional alert covering the region. Reported both as *any signal* and as *expected signal*.
- **Delay:** first matching page minus scenario start, reported **per scenario type** (p50 and p95). A pooled p95 would just be the 6-hour creep.
- **Recall:** detected / should-page, as **k/n with a Wilson 95% interval**, never as a bare percentage.
- **Pages split three ways:**
  - **false pages per day**: pages matching no should-page scenario;
  - **pages per detected scenario**: redundancy from several signals and from tenant pages sent before a regional alert formed;
  - **re-fires per scenario**.
- **Precision:** matched pages / all pages, with counts.
- **Per-scenario table:** detected, delay, pages, whether a regional alert formed, and false pages during each should-not-page scenario.

### 5.6 Selection and the headline *(owner's call)*

- **Selection rule, fixed in advance and applied per seed on week 4:** the setting with the fewest false pages per day among those with recall ≥ 8/10; ties go to the lower median delay. If no setting reaches 8/10, take the highest recall. `results.md` says which branch applied, per family and per seed.
- **Headline:** each family's **pre-registered selection, reported on week 5**: false pages per day and recall (k/n), as the median and range over 5 seeds, at the main setting.
- **Beside it:** a pages-per-day vs recall curve for every setting of every family on week 5, labelled "chosen after seeing the test week; for reference".
- **Statement in `results.md`:** these results test the implementation under the stated traffic assumption; they are not evidence about real traffic.

### 5.7 Event-path equivalence check

- A small world (one region, about 10 keys, 5 weeks) is generated once as events and once as aggregates, from the same random draws.
- Events go through the full events-mode job; aggregates go through aggregates mode.
- **The two alert logs must be identical.** The Python aggregator uses the job's p95 bucket scheme, so the aggregates are identical too.
- This is `make check-paths`, and its result is reported in `results.md`.

## 6. Performance (milestone 5) *(builder's calls)*

- **Throughput:**
  - pre-fill `bench.events` densely, at one key cardinality (about 2 K keys);
  - run the events-mode job from the earliest offset;
  - report the sustained source rate from Flink's REST metrics.
  This measures Flink, not the Python generator.
- **Delay** (live generator at a fixed rate, with many small injected incidents): report both at p50 and p95.
  - **Pipeline delay:** alert append time minus the event-time moment the alert became decidable (end of hold). This is the system's own overhead: watermark bound, minute close, processing, Kafka.
  - **Time-to-page:** alert append time minus incident start in event time. It includes the 3-minute confirmation and the 2-minute hold, so it's at least about 5 minutes for every detector, and the docs say so.
- **State size:** measured from checkpoint statistics after week 5 of an eval run, where 60 keys have a real 4-week history. Larger key counts are extrapolated in `design.md` and labelled as extrapolation.
- **Write-up:** `docs/performance.md` records the hardware, the Docker VM's CPUs and memory, versions, parallelism and the commands.

## 7. Repo layout, tests and CI *(builder's calls)*

```
README.md  LICENSE  Makefile  docker-compose.yml  .github/workflows/ci.yml
flink-job/   Maven, Java 17; package io.github.sanchaypahalwani.tenantbaselines
py/          uv project: generator (events and aggregates), thresholds, forwarder, pager_sink, eval, bench; tests
infra/       clickhouse/, grafana/, alertmanager/, kafka/
docs/        design.md, results.md, performance.md, superpowers/
data/        generated datasets (git-ignored)
```

**Java tests** (JUnit 5, Flink operator test harness):
- robust stats and every floor;
- the 5-minute volume sum;
- the zero-traffic rule;
- hour-of-week bucketing;
- MinuteAggregator: zero-fill, late events, **no re-registration at `Long.MAX_VALUE`**;
- the live watermark: ticks close minutes when events stop, and never outrun a backlog;
- the state machine, including the pause rule;
- cold-start fallback;
- loading the thresholds file and its validity windows;
- RegionGrouper: hold, absorb, regional fire and resolve, re-hold on regional resolve;
- unit drain rules;
- event-time key expiry.

One MiniCluster test runs the full job on a tiny in-memory dataset.

**Python tests** (pytest):
- generator determinism;
- events and aggregates from the same draws agree;
- scenario eligibility and labels;
- threshold fitting with masking;
- Wilson intervals;
- the selection rule;
- eval matching and metrics on hand-made fixtures;
- the forwarder's re-send, resolve, stale-skip and restart logic against a fake Alertmanager.

**CI.** The GitHub Actions workflow has three jobs: `mvn verify` on Temurin 17, `uv sync` + ruff + pytest, and an Alertmanager config check with `amtool`. *(Amended 2026-11-11, builder's call, for milestone 3.)* Nothing is pushed, so it can't run on GitHub yet. `make ci` runs the same commands locally in the same containers, and every milestone report says CI itself hasn't run. `make smoke` (a full compose run) is local only.

## 8. Milestones

Build-time estimates assume coding agents. The total is about 30–40 hours. Each milestone ends with a local commit and a stop for the owner's review.

1. **End to end with static thresholds (~6 h).**
   - Compose stack with named volumes; event generator, live mode.
   - Parsing, watermarks (replay and live), MinuteAggregator with zero-fill and the end-of-input guard.
   - The shared state machine; global static from a config file; the alerts topic.
   - ClickHouse via the Kafka engine; forwarder, Alertmanager and pager-sink; two Grafana panels; `make demo`.
   - Tests for these pieces, the CI workflow, `make ci`.
   - This doc's scenario table (§5.4) is committed before any detector code exists. It already is.
2. **Seasonal baselines (~8 h).**
   - Baseline state; robust stats with the floors; the 5-minute volume sum; the zero-traffic rule; the pause rule.
   - Cold start with `PopulationBaselines`; event-time key expiry.
   - Demo on one continuous timeline.
   - Test-harness tests.
3. **Alert noise control (~4 h).** RegionGrouper (hold, absorb, regional resolve, re-hold), unit status, Alertmanager routes and inhibit rule, alert log.
4. **Eval (~8 h).**
   - Aggregate generator with the traffic model and nuisance; scenario placement; labels.
   - The thresholds script (per-key and global, masked); aggregates mode in the job; all 12 settings side by side.
   - Scoring, selection, Wilson intervals; dev seeds, then the settings freeze, then 5 official seeds and the sweep.
   - `make check-paths`; `docs/results.md`.
5. **Throughput and delay (~3 h).** `make bench`, `docs/performance.md`.
6. **README and design doc (~3 h).**
   - Architecture diagram; the remaining Grafana panels.
   - The reasoning for each choice in this document.
   - Why a single OLAP store.
   - What a much larger deployment would change: how keys are partitioned, hot tenants, older data moved to cheaper storage, running Flink under Kubernetes.
   - What the repo doesn't do.

## 9. Left to the builder

Class and field names; JSON field names; Grafana panel layout; Makefile target names other than `demo`, `eval`, `bench`, `check-paths`, `smoke`, `ci` and `test`; Python package layout inside `py/`; dev seed values.

## 10. Owner's decisions (2026-11-09)

1. Repo name `tenant-baselines`. Flink job in Java 17; generator, forwarder and eval in Python.
2. Hours: on top of the regular weekly budget. All six milestones, simplified (§8), with a review stop after each.
3. Baseline: median + MAD over the last 4 weeks, per hour-of-week bucket.
4. Fire after 3 minutes at ≥ 4; resolve after 5 minutes below 2. Tuned on history weeks only.
5. Cold start: error rate and latency fall back region → global; volume waits.
6. Every minute feeds the baseline.
7. Two static opponents, global and per-key.
8. Regional grouping: hold 2 minutes, promote at ≥ 50%, Alertmanager inhibit as a backstop.
9. Unit status is published only.
10. ClickHouse pulls from Kafka.
11. Docker VM at 8 CPUs / 10 GB.
12. Dev seeds, settings freeze, official runs once each, bug-fix re-runs logged.
13. Detection eval on generated aggregates, with 5 official seeds and a 3-point seasonality sweep.
14. Headline is the pre-registered week-4 selection; the test-week curve sits beside it, labelled.
