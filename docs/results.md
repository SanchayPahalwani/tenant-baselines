# Results

These results test the implementation under the stated traffic assumption; they are not evidence about real traffic. All data here is synthetic: generated traffic, generated incidents, and a generated world.

## Setup

- Runs at commit: `b97503c`
- Command: `make eval`
- Settings frozen at commit: `9f25a23`
- Jar SHA-256: `1dff21f3fcc1c6ba11b72c0400843c5adf9d2a4a2a55ebd3a8371d1046e6e78e`
- Runs: first start 2026-11-12 06:20:37 UTC, last finish 2026-11-12 07:01:08 UTC, elapsed 40.5 minutes (`docs/results/official-runs.jsonl`)
- Machine: Apple M3 Pro, 12 cores, 18.0 GiB memory
- Docker VM: Docker 28.4.0, 8 CPUs, 9.7 GiB memory
- Machine facts: from the Setup block that `make eval` rendered right after the official runs on 2026-11-12 (`docs/results.md` at commit `7fba0be`, whose Setup names commit `b97503c`)
- Versions: Flink 2.2.1 (Java 17); Flink Kafka connector 5.0.0-2.2; Kafka 4.4.0; ClickHouse 26.8; Grafana 13.2.5; Alertmanager v0.34.2; Python 3.12.15; numpy 2.5.4
- Rendered at commit `3233c9e` on 2026-11-12
- Measured code at the render commit equals the freeze commit: yes, except display-only files: infra/grafana/dashboards/tenant-baselines.json

## Headline: 6:1 day/night amplitude, selected settings on the test week

Seeds at this amplitude: 5. Each seed picks its own setting on week 4, and the result below is that setting on week 5. Recall counts a scenario when a page is on its expected signal; the any-signal column counts a page on any signal. Intervals are Wilson intervals at the 95 percent level, written as fractions.

| Family | Expected-signal recall per seed: median (range) | Expected-signal recall, pooled k/n (interval) | Any-signal recall, pooled k/n (interval) | False pages per day per seed: median (range) | False pages per day, pooled |
|---|---|---|---|---|---|
| seasonal | 9 (8–10) of 10 | 45/50 (0.79–0.96) | 45/50 (0.79–0.96) | 9.86 (7.86–11.14) | 9.63 |
| per key static | 7 (7–9) of 10 | 38/50 (0.63–0.86) | 38/50 (0.63–0.86) | 23.86 (13.43–27.00) | 21.20 |
| global static | 7 (5–7) of 10 | 31/50 (0.48–0.74) | 31/50 (0.48–0.74) | 13.14 (5.29–17.71) | 12.69 |

Selected setting and branch of the selection rule, per family and seed:

| Family | Seed | Setting | Branch |
|---|---|---|---|
| seasonal | 4101 | k6 | reached_8_of_10 |
| seasonal | 4102 | k6 | reached_8_of_10 |
| seasonal | 4103 | k6 | reached_8_of_10 |
| seasonal | 4104 | k6 | reached_8_of_10 |
| seasonal | 4105 | k6 | reached_8_of_10 |
| per key static | 4101 | p99.9 | reached_8_of_10 |
| per key static | 4102 | p99 | highest_recall |
| per key static | 4103 | p99.9 | reached_8_of_10 |
| per key static | 4104 | p99 | reached_8_of_10 |
| per key static | 4105 | p99 | reached_8_of_10 |
| global static | 4101 | p99 | reached_8_of_10 |
| global static | 4102 | p99.9 | highest_recall |
| global static | 4103 | p99 | highest_recall |
| global static | 4104 | p99 | highest_recall |
| global static | 4105 | p99 | reached_8_of_10 |

## Sweep over the day/night amplitude

| Day/night amplitude | Family | Seeds | Expected-signal recall, pooled | Any-signal recall, pooled | False pages per day: median (range); pooled |
|---|---|---|---|---|---|
| 1.5:1 | seasonal | 5 | 50/50 (0.93–1.00) | 50/50 (0.93–1.00) | 14.00 (12.57–14.71); pooled 13.63 |
| 1.5:1 | per key static | 5 | 38/50 (0.63–0.86) | 38/50 (0.63–0.86) | 13.57 (11.43–19.86); pooled 14.43 |
| 1.5:1 | global static | 5 | 36/50 (0.58–0.83) | 36/50 (0.58–0.83) | 15.57 (5.14–16.71); pooled 12.46 |
| 6:1 | seasonal | 5 | 45/50 (0.79–0.96) | 45/50 (0.79–0.96) | 9.86 (7.86–11.14); pooled 9.63 |
| 6:1 | per key static | 5 | 38/50 (0.63–0.86) | 38/50 (0.63–0.86) | 23.86 (13.43–27.00); pooled 21.20 |
| 6:1 | global static | 5 | 31/50 (0.48–0.74) | 31/50 (0.48–0.74) | 13.14 (5.29–17.71); pooled 12.69 |
| 12:1 | seasonal | 5 | 43/50 (0.74–0.93) | 43/50 (0.74–0.93) | 9.43 (8.57–11.14); pooled 9.60 |
| 12:1 | per key static | 5 | 39/50 (0.65–0.87) | 39/50 (0.65–0.87) | 28.71 (15.57–30.43); pooled 26.09 |
| 12:1 | global static | 5 | 31/50 (0.48–0.74) | 31/50 (0.48–0.74) | 18.71 (2.14–22.71); pooled 15.57 |

## Every setting on the test week (chosen after seeing the test week; for reference)

Main setting (6:1), all seeds pooled. Pages and false pages are per day. Precision is matched pages over all pages.

| Family | Setting | Pages per day | False pages per day | Expected-signal recall | Any-signal recall | Precision (matched/all) |
|---|---|---|---|---|---|---|
| seasonal | k3 | 90.91 | 87.94 | 46/50 (0.81–0.97) | 46/50 (0.81–0.97) | 104/3182 |
| seasonal | k4 | 37.14 | 34.57 | 46/50 (0.81–0.97) | 46/50 (0.81–0.97) | 90/1300 |
| seasonal | k5 | 17.40 | 15.34 | 46/50 (0.81–0.97) | 46/50 (0.81–0.97) | 72/609 |
| seasonal | k6 | 11.71 | 9.63 | 45/50 (0.79–0.96) | 45/50 (0.79–0.96) | 73/410 |
| per key static | p99 | 27.74 | 26.00 | 39/50 (0.65–0.87) | 39/50 (0.65–0.87) | 61/971 |
| per key static | p99.9 | 15.66 | 13.89 | 37/50 (0.60–0.84) | 37/50 (0.60–0.84) | 62/548 |
| per key static | p99.97 | 9.17 | 8.17 | 25/50 (0.37–0.63) | 25/50 (0.37–0.63) | 35/321 |
| per key static | p99.99 | 5.89 | 5.11 | 21/50 (0.29–0.56) | 21/50 (0.29–0.56) | 27/206 |
| global static | p99 | 15.54 | 13.97 | 32/50 (0.50–0.76) | 32/50 (0.50–0.76) | 55/544 |
| global static | p99.9 | 6.86 | 5.77 | 27/50 (0.40–0.67) | 27/50 (0.40–0.67) | 38/240 |
| global static | p99.97 | 4.20 | 3.46 | 22/50 (0.31–0.58) | 22/50 (0.31–0.58) | 26/147 |
| global static | p99.99 | 2.03 | 1.60 | 13/50 (0.16–0.40) | 13/50 (0.16–0.40) | 15/71 |

## By scenario type

Each family's selected setting on week 5 at 6:1, pooled over seeds. Delay is minutes from scenario start to the first page on the expected signal: p50 / p95 by nearest rank, with the count. Pages per detected scenario counts every matching page of a scenario that was detected on any signal. The regional column is the number of scenarios in whose window a regional alert formed.

| Family | Scenario type | Expected-signal recall | Any-signal recall | Delay p50 / p95, minutes | Pages per detected | Re-fires | Regional alert formed | False pages during |
|---|---|---|---|---|---|---|---|---|
| seasonal | regional_outage | 5/5 (0.57–1.00) | 5/5 (0.57–1.00) | 5.0 / 5.0 (n=5) | 3.00 | 0 | 5/5 | n/a |
| seasonal | drop_work | 5/5 (0.57–1.00) | 5/5 (0.57–1.00) | 8.0 / 10.0 (n=5) | 1.00 | 0 | 0/5 | n/a |
| seasonal | drop_night | 3/5 (0.23–0.88) | 3/5 (0.23–0.88) | 20.0 / 24.0 (n=3) | 1.00 | 0 | 0/5 | n/a |
| seasonal | latency_creep | 10/10 (0.72–1.00) | 10/10 (0.72–1.00) | 142.0 / 255.0 (n=10) | 2.80 | 1 | 5/10 | n/a |
| seasonal | offhours_latency | 10/10 (0.72–1.00) | 10/10 (0.72–1.00) | 5.0 / 11.0 (n=10) | 1.00 | 0 | 0/10 | n/a |
| seasonal | error_burst | 10/10 (0.72–1.00) | 10/10 (0.72–1.00) | 5.0 / 13.0 (n=10) | 1.00 | 0 | 0/10 | n/a |
| seasonal | cold_start_burst | 2/5 (0.12–0.77) | 2/5 (0.12–0.77) | 5.0 / 5.0 (n=2) | 1.00 | 0 | 0/5 | n/a |
| seasonal | launch_surge (should not page; 5 scenarios) | n/a | n/a | n/a | n/a | n/a | 0/5 | 10 |
| seasonal | holiday_dip (should not page; 5 scenarios) | n/a | n/a | n/a | n/a | n/a | 5/5 | 18 |
| per key static | regional_outage | 5/5 (0.57–1.00) | 5/5 (0.57–1.00) | 5.0 / 5.0 (n=5) | 2.20 | 0 | 5/5 | n/a |
| per key static | drop_work | 0/5 (0.00–0.43) | 0/5 (0.00–0.43) | none (n=0) | none | 0 | 0/5 | n/a |
| per key static | drop_night | 2/5 (0.12–0.77) | 2/5 (0.12–0.77) | 5.0 / 14.0 (n=2) | 1.00 | 0 | 0/5 | n/a |
| per key static | latency_creep | 9/10 (0.60–0.98) | 9/10 (0.60–0.98) | 89.0 / 290.0 (n=9) | 2.78 | 0 | 6/10 | n/a |
| per key static | offhours_latency | 9/10 (0.60–0.98) | 9/10 (0.60–0.98) | 5.0 / 17.0 (n=9) | 1.00 | 0 | 0/10 | n/a |
| per key static | error_burst | 10/10 (0.72–1.00) | 10/10 (0.72–1.00) | 5.0 / 8.0 (n=10) | 1.00 | 0 | 0/10 | n/a |
| per key static | cold_start_burst | 3/5 (0.23–0.88) | 3/5 (0.23–0.88) | 5.0 / 5.0 (n=3) | 1.00 | 0 | 0/5 | n/a |
| per key static | launch_surge (should not page; 5 scenarios) | n/a | n/a | n/a | n/a | n/a | 0/5 | 8 |
| per key static | holiday_dip (should not page; 5 scenarios) | n/a | n/a | n/a | n/a | n/a | 1/5 | 41 |
| global static | regional_outage | 5/5 (0.57–1.00) | 5/5 (0.57–1.00) | 5.0 / 5.0 (n=5) | 2.40 | 0 | 5/5 | n/a |
| global static | drop_work | 0/5 (0.00–0.43) | 0/5 (0.00–0.43) | none (n=0) | none | 0 | 0/5 | n/a |
| global static | drop_night | 0/5 (0.00–0.43) | 0/5 (0.00–0.43) | none (n=0) | none | 0 | 0/5 | n/a |
| global static | latency_creep | 8/10 (0.49–0.94) | 8/10 (0.49–0.94) | 123.0 / 358.0 (n=8) | 2.62 | 0 | 6/10 | n/a |
| global static | offhours_latency | 4/10 (0.17–0.69) | 4/10 (0.17–0.69) | 5.0 / 14.0 (n=4) | 1.00 | 0 | 0/10 | n/a |
| global static | error_burst | 10/10 (0.72–1.00) | 10/10 (0.72–1.00) | 5.0 / 11.0 (n=10) | 1.00 | 0 | 0/10 | n/a |
| global static | cold_start_burst | 4/5 (0.38–0.96) | 4/5 (0.38–0.96) | 5.0 / 13.0 (n=4) | 1.00 | 0 | 0/5 | n/a |
| global static | launch_surge (should not page; 5 scenarios) | n/a | n/a | n/a | n/a | n/a | 0/5 | 8 |
| global static | holiday_dip (should not page; 5 scenarios) | n/a | n/a | n/a | n/a | n/a | 3/5 | 17 |

Regional outages on week 5, with their hour (the hour is reported, spec 5.4):

| Seed | Region | Start (UTC) | Start (local) | seasonal | per key static | global static |
|---|---|---|---|---|---|---|
| 4101 | eu-central | Fri 18:12 UTC | Fri 19:12 local | k6: expected-signal page, delay 5.0 min, regional alert, 2 pages | p99.9: expected-signal page, delay 5.0 min, regional alert, 2 pages | p99: expected-signal page, delay 5.0 min, regional alert, 1 page |
| 4102 | eu-central | Wed 11:34 UTC | Wed 12:34 local | k6: expected-signal page, delay 5.0 min, regional alert, 6 pages | p99: expected-signal page, delay 5.0 min, regional alert, 2 pages | p99.9: expected-signal page, delay 5.0 min, regional alert, 2 pages |
| 4103 | ap-southeast | Thu 21:08 UTC | Fri 05:08 local | k6: expected-signal page, delay 5.0 min, regional alert, 1 page | p99.9: expected-signal page, delay 5.0 min, regional alert, 2 pages | p99: expected-signal page, delay 5.0 min, regional alert, 3 pages |
| 4104 | us-east | Mon 12:55 UTC | Mon 07:55 local | k6: expected-signal page, delay 5.0 min, regional alert, 2 pages | p99: expected-signal page, delay 5.0 min, regional alert, 4 pages | p99: expected-signal page, delay 5.0 min, regional alert, 5 pages |
| 4105 | eu-central | Tue 23:08 UTC | Wed 00:08 local | k6: expected-signal page, delay 5.0 min, regional alert, 4 pages | p99: expected-signal page, delay 5.0 min, regional alert, 1 page | p99: expected-signal page, delay 5.0 min, regional alert, 1 page |

## Event-path equivalence check

`make check-paths` passed.

A synthetic world (`paths (synthetic)`) went through the job once as 25,780,861 events and once as 443,520 minute aggregates. The alert logs compared equal: true. The aggregates compared equal: true. The alert log holds 2,583 rows, 62 of them regional firing rows.

Run at commit `e57f621` on 2026-11-12T08:53:03Z. Recorded: run id `paths-s7-a6-20261112T080636`; a clean tree; jar SHA-256 `1dff21f3fcc1c6ba11b72c0400843c5adf9d2a4a2a55ebd3a8371d1046e6e78e`. Seconds: events job 2663.6, aggregates job 54.3, total 2787.4.

## Method notes

- **Seeds.** The code enforces the seed protocol. A dev run refuses any seed outside 7, 11 and 13. An official run needs the freeze file, uses only the seeds listed in it, and refuses a seed and amplitude that already has an official run. Official seeds: 4101, 4102, 4103, 4104, 4105. The settings were frozen at commit `9f25a23`. This render holds runs of kind off at seeds 4101, 4102, 4103, 4104, 4105.
- **The `dirty` flag.** Each of the 15 official summaries records `dirty: true`. The flag counts the driver's own registry line, written before the run. The driver refuses any other change (`eval_official.preflight`).
- **The world.** 3 regions, 24 tenants (8 per region) and 48 keys: 6 shared units and 6 dedicated units. The 18 shared tenants have a key on each of the two shared units of their region (36 keys). The 6 dedicated tenants have their own unit and one key on the region's first shared unit (12 keys). The spec says about 60 keys, but its two sentences give 42. Reaching 60 would need a third shared unit or keys across regions, and cross-region keys would put half the tenants partly out of their time zone, so the world has 48. Five tenants keep the hours of another region's time zone.
- **Recall.** Every recall figure is expected-signal recall unless the column says any-signal. A scenario counts when a page lands on the signal the scenario is meant to move. The selection rule on week 4 uses the same count.
- **Selection ties.** Among the settings that reach 8 of 10 (or, when none does, among those with the highest recall), the rule takes the fewest false pages per day, then the lower median delay, then the less sensitive setting. Spec 5.6 names the first two; the third is the builder's call, recorded in `eval/freeze.json`.
- **Matched and false pages.** On week 5 at 6:1, pooled over the seeds. Most pages are false in every family: at the selected settings, 73 of 410 pages matched a scenario (seasonal), 60 of 802 (per key static) and 51 of 495 (global static). The traffic model's unlabelled bursts and per-day level factors (spec 5.2) are the expected cause; this eval does not measure the split.
- **The thresholds mask.** The static thresholds are fitted with every labelled scenario masked out, except the Monday peak. Launch surges and holiday dips are masked too. The mask covers the scenario's keys from its start to 4 minutes past its end, because the five-minute sum carries a scenario's effect for 4 more minutes.
- **The clip at the replay end.** The replay ends at the end of week 5. Alert rows with an event time past it are dropped before scoring: 6 rows over all runs here. Alerts still open at the end count as pages: 388 over all runs here. Duplicate rows dropped: 0.
- **Inclusive fire comparison.** The shared state machine fires at or past the fire level (spec 4.5). The values are discrete (bucketed p95, error counts over request counts, whole 5-minute sums), so a static threshold is breached on calibration minutes more often than its nominal percentile. Static false pages therefore run somewhat higher than the percentile alone suggests. The comparison was not changed.
- **What check-paths shows.** It shows that the events path and the aggregates path give the same alert log. It does not show that the detectors are right: both paths share the thresholds, every operator from the minute aggregates on, and the random draws.
- **Measured code.** Changed 2026-11-12, after the official runs: the dashboard files under infra/grafana/dashboards may differ from the freeze commit, and the Setup line names each one that does. The dashboards only draw; the eval reads pages from the alerts topic. Everything else under infra/, and docker-compose.yml, is still compared.
- **Replay mode can fail.** Replay mode aligns the watermarks of the Kafka partitions. That can hit a race in the Kafka connector (FLINK-39728). It is fixed in connector 5.1.0, which is not released as of 2026-11-12; this repo pins 5.0.0-2.2. A run that hits it fails and gives no output, so no wrong result is possible, and check-paths can be run again. The official runs use aggregates mode. It has one partition and no alignment, so it cannot hit the race.
- **Events-path speed.** The events path of check-paths is bound by the alignment: about 44 minutes for five weeks. The alignment drift was not changed.
- **Single-key creeps.** A latency creep is placed on one unit. A dedicated unit is a unit, so a creep can land on a unit with a single key.
- **No Monday holiday in two regions.** eu-central and ap-southeast never get a holiday dip on a Monday. Their local Monday starts before the UTC week, so that local day cannot lie inside the week.
- **The traffic model and the spreads were kept as specified.** The owner reserved the decision at the freeze and then delegated it to the builder. The builder left spec 5.2 and 4.3 as written. Changing them after seeing dev results would be tuning.
- **Changes to the job or the run mechanics.** None were passed to this render (alignment drift, scorer writes and state backend are as in the commit above).

## Run log

1. `b97503c`, 2026-11-12: first official run: 15 runs, 5 seeds at 3 amplitudes. Why: the settings freeze at `9f25a23`. Old headline: none. New headline: seasonal 45/50, 9.63 false pages per day; per key static 38/50, 21.20; global static 31/50, 12.69.
