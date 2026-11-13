# Performance

## What this is

Three measurements on one laptop, on synthetic data: how fast the job reads a filled topic, how long an alert takes from an injected incident to the alerts topic, and how much state the job keeps per key. They describe this build on this machine. They are not evidence about any real system or any larger deployment.

## Machine and versions

These figures describe the machine that produced the synthetic runs below. Every row is read from the result files' environment records. There is one column per measurement when any row differs between the files.

| Item | Throughput | Delay | State |
| --- | --- | --- | --- |
| Date (UTC) | 2026-11-12T15:32:47+00:00 | 2026-11-12T15:46:46+00:00 | 2026-11-12T17:04:55+00:00 |
| Commit | e60416eff7b5e521c6b3c08a06656624dfd7bbd2 | e60416eff7b5e521c6b3c08a06656624dfd7bbd2 | e60416eff7b5e521c6b3c08a06656624dfd7bbd2 |
| CPU | Apple M3 Pro | Apple M3 Pro | Apple M3 Pro |
| Cores | 12 | 12 | 12 |
| Memory (GB) | 18.0 | 18.0 | 18.0 |
| OS | 26.5.2 | 26.5.2 | 26.5.2 |
| Docker VM CPUs | 8 | 8 | 8 |
| Docker VM memory (GB) | 9.7 | 9.7 | 9.7 |
| Other containers in the VM | 12 | 12 | 12 |
| Images | apache/kafka:4.4.0, clickhouse/clickhouse-server:26.8.36.3, flink:2.2.1-java17, grafana/grafana:13.2.5, prom/alertmanager:v0.34.2, tenant-baselines-forwarder, tenant-baselines-pager-sink | apache/kafka:4.4.0, clickhouse/clickhouse-server:26.8.36.3, flink:2.2.1-java17, grafana/grafana:13.2.5, prom/alertmanager:v0.34.2, tenant-baselines-forwarder, tenant-baselines-pager-sink | apache/kafka:4.4.0, clickhouse/clickhouse-server:26.8.36.3, flink:2.2.1-java17, grafana/grafana:13.2.5, prom/alertmanager:v0.34.2, tenant-baselines-forwarder, tenant-baselines-pager-sink |
| Flink version | 2.2.1 | 2.2.1 | 2.2.1 |
| Parallelism | 2 | 2 | 2 |
| Task slots | 4 | 4 | 4 |
| TaskManager memory | 2560m | 2560m | 2560m |
| Power at start and end | AC Power at the start, AC Power at the end | AC Power at the start, AC Power at the end | AC Power at the start, AC Power at the end |

Power is read when a run starts and when it ends, not in between. The OS power log, in the next section, shows what came between.

The state runs' backend and checkpoint interval are in the state-size section, not in this table.

## Power during the runs

This section is about the machine during the synthetic runs above, not about the data. The drivers read the power source only when a run starts and when it ends. The OS also logs every change of power source: this section reads the lines of `pmset -g log` that say `Using AC` or `Using Batt`, saved during the sitting under `docs/performance/power/`. The table lists each source the log shows during each run, with its seconds inside the run. Times are the log's local time, IST (UTC+05:30). A run's end is when its driver wrote its result file; the result files do not record an end time.

| Measurement | Run | Start (IST) | End (IST) | Power source in the OS log |
| --- | --- | --- | --- | --- |
| Throughput | tp-20261112T153247 | 21:02:47 | 21:16:44 | AC Power, 837 s |
| Delay | delay-20261112T154647 | 21:16:46 | 22:34:54 | AC Power, 287 s; Battery Power, 330 s (21:21:33 to 21:27:03); AC Power, 4,071 s |
| State | state-20261112T170455 | 22:34:55 | 22:38:35 | AC Power, 220 s |

The delay run (delay-20261112T154647) had the charger out from 21:21:33 to 21:27:03 IST, for 330 s, and ran on battery. In the run's own records, the generator had written the history at 21:20:54, the job caught up on it at 21:25:29, the 300 s settle period ended at 21:25:54 and the first incident started at 21:26:40. So the charger was out for the job's catch-up from 21:21:33 on, the rest of the settle period and the first 23 s of the first incident. No incident was paged on battery: the earliest a page matched to an incident could be appended is the first incident's start plus the shortest time-to-page of any setting, 269.4 s, at 21:31:09, 246 s after the charger came back. The delay pilot delay-20261112T143936 was on battery at its start and at its end; it gave a pipeline delay of 21.5 s at p50 and 22.6 s at p95, against 21.6 s and 22.5 s in this run.

## Throughput

This section uses synthetic data. The fill had 2,000 keys at 120 requests a minute each, for 240 event minutes: 57,590,352 events in 8 partitions. The generator filled the topic at 682,318 events a second (84.4 s), so the job's rate can be set beside the rate at which the same events were written.

The topic was complete before the job started, and nothing produced during the run. The rate is the rise of the source's record counter over a window that leaves out the first 60 s after the first sample that showed a count and the last 10% of the fill, sampled every 12 s.

| Run | Sustained events a second | Source's own clock, events a second | Window (s) | Lowest to highest interval rate | Source back-pressured | Source idle | Stale / re-read samples |
| --- | --- | --- | --- | --- | --- | --- | --- |
| tp-20261112T153247-r1 | 179,224 | 179,221 | 192.0 | 132,569.6 to 193,694.4 | 87.5% | 0.2% | 0 / 0 |
| tp-20261112T153247-r2 | 151,897 | 151,829 | 240.0 | 106,655.3 to 193,124.3 | 87.2% | 0.2% | 0 / 0 |

A source that is neither busy nor back-pressured is waiting on the broker. The source was back-pressured 87.5% of the window and idle 0.2% in tp-20261112T153247-r1, and back-pressured 87.2% and idle 0.2% in tp-20261112T153247-r2: the job set the pace, not the broker.

The two runs differ by 16.5% of their mean. tp-20261112T153247-r1 was the faster run. The first job on a TaskManager starts colder, which would make it the slower one. Here it was the faster, so the order does not explain the spread. The source's own clock is a cross-check that does not depend on the host's timing.

`timing_error_pct` is a bound, computed from the longest measured read delay at the window's two ends over the window: tp-20261112T153247-r1: 1.6%; tp-20261112T153247-r2: 1.3%.

The busiest vertex: tp-20261112T153247-r1: minute-aggregator -> (aggregates-sink: Writer -> aggregates-sink: Committer, late-events -> late-events-discard: Writer) at 98.7%; tp-20261112T153247-r2: minute-aggregator -> (aggregates-sink: Writer -> aggregates-sink: Committer, late-events -> late-events-discard: Writer) at 98.8%. Every vertex's busy, back-pressured and idle share, as a share of parallelism times the window:

| Vertex | tp-20261112T153247-r1 busy | tp-20261112T153247-r1 back-pressured | tp-20261112T153247-r1 idle | tp-20261112T153247-r2 busy | tp-20261112T153247-r2 back-pressured | tp-20261112T153247-r2 idle |
| --- | --- | --- | --- | --- | --- | --- |
| Source: events | 12.3% | 87.5% | 0.2% | 12.6% | 87.2% | 0.2% |
| Source: ticks | 0.1% | 0.0% | 99.9% | 0.2% | 0.0% | 99.9% |
| drop-ticks | 6.0% | 94.0% | 0.0% | 6.0% | 94.0% | 0.0% |
| minute-aggregator -> (aggregates-sink: Writer -> aggregates-sink: Committer, late-events -> late-events-discard: Writer) | 98.7% | 0.0% | 1.4% | 98.8% | 0.0% | 1.2% |
| population-baselines | 5.3% | 0.0% | 94.7% | 5.7% | 0.0% | 94.3% |
| region-grouper -> (alerts-sink: Writer -> alerts-sink: Committer, unit-status-sink: Writer -> unit-status-sink: Committer) | 2.8% | 0.0% | 97.2% | 2.8% | 0.0% | 97.3% |
| scorer -> scores-sink: Writer -> scores-sink: Committer | 11.7% | 0.0% | 88.3% | 11.7% | 0.0% | 88.3% |

The `drop-ticks` vertex's busy share, as a share of parallelism times the window, is tp-20261112T153247-r1: 6.0%; tp-20261112T153247-r2: 6.0%. It is reported as a share of CPU time, not as a throughput difference. The hop adds one read of each event, the filter and one write. The drop-ticks task's busy time is those plus choosing the key's channel, which the source would do without the union, so the share is an upper bound on what the hop adds. The task's own share: tp-20261112T153247-r1: back-pressured 94.0% of the window; tp-20261112T153247-r2: back-pressured 94.0% of the window. It was back-pressured for more than half of the window in every run, so it did not set the pace.

The job ran 3 settings at once; the bench world's keys have no per-key entry in `thresholds-demo.json`, so `per_key_static` used the global levels. The alert rows each run wrote, counted from `bench.alerts`:

No run wrote an alert row: the bench world is built to stay under every fire level.

The TaskManager's memory against its limit. The first reading is for one job; the second is cumulative, taken after the TaskManager had already run the first job:

- tp-20261112T153247-r1: 1.90 GiB of 2.50 GiB (76.0%)
- tp-20261112T153247-r2: 1.99 GiB of 2.50 GiB (79.6%)

The last checkpoint's size per vertex, in bytes, at 2,000 keys:

| Vertex | tp-20261112T153247-r1 (checkpoint 12) | tp-20261112T153247-r2 (checkpoint 13) |
| --- | --- | --- |
| population-baselines | 75,709 | 69,758 |
| Source: events | 940 | 940 |
| region-grouper -> (alerts-sink: Writer -> alerts-sink: Committer, unit-status-sink: Writer -> unit-status-sink: Committer) | 1,513,835 | 1,094,377 |
| Source: ticks | 265 | 265 |
| minute-aggregator -> (aggregates-sink: Writer -> aggregates-sink: Committer, late-events -> late-events-discard: Writer) | 2,195,403 | 2,976,790 |
| scorer -> scores-sink: Writer -> scores-sink: Committer | 10,283,842 | 10,109,873 |
| drop-ticks | 0 | 0 |
| Whole checkpoint | 14,069,994 | 14,252,003 |

The fill is 240 event minutes, so no key has the history the seasonal detector needs and every key is scored against the fallback baselines. No unit/tenant pair goes stale in this world. So these sizes are not the state of a warmed-up job.

What shares the VM: the broker, ClickHouse reading the bench topics, and 12 other containers when the run began.

What the machine was doing:

| Reading | Host load, 1 min | This project's containers, CPU | Other containers, CPU | Scale |
| --- | --- | --- | --- | --- |
| tp-20261112T153247-r1 start | 7.25 | 163.9 | 65.8 | 8 VM CPUs; percent of one CPU |
| tp-20261112T153247-r1 end | 7.91 | 29.9 | 20.3 | 8 VM CPUs; percent of one CPU |
| tp-20261112T153247-r2 start | 6.47 | 21.9 | 12.7 | 8 VM CPUs; percent of one CPU |
| tp-20261112T153247-r2 end | 7.53 | 90.2 | 16.5 | 8 VM CPUs; percent of one CPU |

The Docker VM is shared. At the readings above, other projects' containers used 12.7 to 65.8 percent of one CPU (the VM has 8 CPUs, so 800 in all), so run-to-run rates can differ.

## Delay

This section uses synthetic data. The world has 36 keys at up to 40 requests a minute per key, in 8 partitions, with 21 days of history before live traffic. The run injected 60 incidents, each an error-rate rise of 20 points on one key for 360 s. The first starts 300 s after the generator catches up; incidents in one region are 180 s apart; the run reads for 180 s after the last one ends.

Pipeline delay is the time from a row's event time, which for a tenant alert is the end of its hold, to the broker's append time of that row in `bench.alerts`. Time-to-page is the time from the incident's start to the append of the first matching row. Percentiles are nearest-rank, with their `n`. Times are in seconds. The live setting is seasonal/k4.

| Detector / setting | Incidents | Matched | Pipeline delay p50 | Pipeline delay p95 | Time to page p50 | Time to page p95 | Time to page minimum |
| --- | --- | --- | --- | --- | --- | --- | --- |
| global_static/p99.9 | 60 | 60 | 21.6 (n=60) | 22.5 (n=60) | 309.4 (n=60) | 364.4 (n=60) | 269.4 (n=60) |
| per_key_static/p99.9 | 60 | 60 | 21.6 (n=60) | 22.5 (n=60) | 309.4 (n=60) | 364.4 (n=60) | 269.4 (n=60) |
| seasonal/k4 (the live setting) | 60 | 60 | 21.6 (n=60) | 22.5 (n=60) | 296.7 (n=60) | 324.4 (n=60) | 269.4 (n=60) |

The delay world's keys have no per-key entry in `thresholds-demo.json`, so `per_key_static` falls back to the global level and its row equals `global_static`'s.

How time-to-page is built (configured, not measured): the detector fires after 3 scored minutes in a row, and the alert is then held for 120 s. Both are counted from the start of the first breaching minute, not from the incident's start. A spike of this size is expected to breach in the minute it starts in, so the count often starts up to a minute before the incident. For the live setting, seasonal/k4, the measured minimum is 269.4 s, and 35 of 60 matched incidents were paged in under five minutes from their start.

The watermark lag, measured during the run and corrected by the VM clock's offset (before 0.068 s, after 0.096 s): minimum 21.3 s, median 21.8 s, maximum 23.1 s over 68 samples. By design (configured, not measured), the lag is the 20 s watermark bound plus the age of the newest event on the slowest partition that is not idle. This benchmark sends at a fixed rate, so every partition is busy. A sparse partition adds up to 60 s (the idleness timeout, configured); that case was not measured here.

Both figures end at the append to the alerts topic. The forwarder and Alertmanager's `group_wait` (30 s, configured) come after it. Page times read at the webhook include the forwarder and `group_wait`, and are not comparable.

The split of the live setting's pipeline delay, in seconds: decision to emit 21.6 at p50 and 22.5 at p95; emit to append 0.0 at p50 and 0.0 at p95.

For the live setting: 0 incidents were missed, 0 firing rows matched no incident, and 0 regional firing rows were written.

What the machine was doing:

| Reading | Host load, 1 min | This project's containers, CPU | Other containers, CPU | Scale |
| --- | --- | --- | --- | --- |
| start | 7.16 | 123.7 | 45.0 | 8 VM CPUs; percent of one CPU |
| end | 4.79 | 32.4 | 24.7 | 8 VM CPUs; percent of one CPU |

## State size

This section uses synthetic data. The world is `eval` (seed 7, amplitude 6.0) with 48 keys, 46 of which run all 5 weeks. 2,358,720 aggregate rows went through the job in aggregates mode.

The hashmap run (the per-key headline): checkpoint 4 was the latest completed one triggered before the source vertex was reported finished, at 97% of the run, interval 5,000 ms, total state 33,897,269 B (32.3 MiB). The scorer holds 29,867,428 B (28.5 MiB), which is 622,238 bytes per key over 48 keys. The spec's estimate is 604,800 bytes per key (an estimate, not a measurement). Method note: checkpoints after tasks finish: true.

| Vertex | State size (bytes) |
| --- | --- |
| population-baselines | 3,975,618 |
| scorer | 29,867,428 |
| Source: aggregates | 234 |
| region-grouper -> (alerts-sink: Writer -> alerts-sink: Committer, unit-status-sink: Writer -> unit-status-sink: Committer) | 53,989 |

Over the last stretch of the run the total state size was 33,897,269 bytes (one checkpoint).

The RocksDB run, with incremental checkpoints (the stack's backend): checkpoint 31 was the latest completed one triggered before the source vertex was reported finished, at 100% of the run, interval 5,000 ms, total state 15,038,049 B (14.3 MiB). The scorer holds 13,765,490 B (13.1 MiB), which is 286,781 bytes per key over 48 keys. The spec's estimate is 604,800 bytes per key (an estimate, not a measurement). Method note: checkpoints after tasks finish: true.

| Vertex | State size (bytes) |
| --- | --- |
| population-baselines | 819,369 |
| scorer | 13,765,490 |
| Source: aggregates | 234 |
| region-grouper -> (alerts-sink: Writer -> alerts-sink: Committer, unit-status-sink: Writer -> unit-status-sink: Committer) | 452,956 |

Over the 6 checkpoints of the last stretch of the run, the total state size ranged from 14,371,599 to 15,061,791 bytes.
The figure moves from one checkpoint to the next: the state is still growing in the last stretch of the run, and SST files that are not yet compacted are counted.

The two backends store the same state in different formats. The hashmap figure is 2.2 times the RocksDB figure. RocksDB keeps its SST files compressed: Flink's default for `state.backend.rocksdb.compression.per.level` is `SNAPPY_COMPRESSION` on every level, and this stack does not set it (a documented default, not measured here).

This job ran against 12 settings; the live job runs 3. The baseline history, which all settings share, is most of a key's state.

## Extrapolation, not measured

This section is an extrapolation from synthetic measurements, not a measurement. Bytes are the scorer's bytes per key times the key count.

| Keys | hashmap (bytes) | rocksdb (bytes) |
| --- | --- | --- |
| 2,000 | 1,244,476,167 | 573,562,083 |
| 20,000 | 12,444,761,667 | 5,735,620,833 |
| 100,000 | 62,223,808,333 | 28,678,104,167 |

It assumes every key has a full 5-week history and that state grows linearly with keys. 2 of the 48 measured keys ran fewer weeks, so the per-key figure is slightly under a full key's. Neither assumption was tested above the measured key count. It leaves out the region grouper's per-key activity, the minute aggregator's state in events mode and the population baselines.

## Pilot runs (not the published figures)

These pilot runs use synthetic data. They are the result files of the pilots that ran to a result, committed unedited in `docs/performance/pilots/`. None is in the figures above; each is shown as it is.

| Run | Commit | Date (UTC) | Power at start and end | Valid | Event minutes | Rate, live setting's delay or bytes per key | Other containers' CPU, start → end (percent of one CPU) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| tp-20261112T094926 | 8ac86e8 | 2026-11-12T09:49:26+00:00 | Battery Power at the start, not recorded at the end | yes | 30 | 13,102 events a second | not recorded |
| tp-20261112T100846 | 56a6fda | 2026-11-12T10:08:46+00:00 | Battery Power at the start, not recorded at the end | yes | 30 | 17,767 events a second | not recorded |
| tp-20261112T103845 | cf2a8a1 | 2026-11-12T10:38:44+00:00 | AC Power at the start, not recorded at the end | no | 30 | no window | not recorded |
| tp-20261112T104138 | cf2a8a1 | 2026-11-12T10:41:38+00:00 | AC Power at the start, not recorded at the end | yes | 90 | 66,533 events a second | not recorded |
| tp-20261112T110455 | 5dd2f93 | 2026-11-12T11:04:55+00:00 | Battery Power at the start, Battery Power at the end | no | 30 | no window; 104,040 events a second on the source's clock | not recorded |
| tp-20261112T111814 | 0556053 | 2026-11-12T11:18:14+00:00 | AC Power at the start, AC Power at the end | yes | 30 | 31,340 events a second | 69.1 → 154.8 |
| delay-20261112T143936 | e4c9575 | 2026-11-12T14:39:35+00:00 | Battery Power at the start, Battery Power at the end | yes |  | pipeline delay p50 21.5 s, p95 22.6 s (n=6) | 55.6 → 49.7 |
| state-20261112T150705 | e4c9575 | 2026-11-12T15:07:05+00:00 | Battery Power at the start, Battery Power at the end | yes |  | hashmap 622,238 bytes per key; rocksdb 275,736 bytes per key | 164.5 → 29.1; 20.3 → 56.4 |

Every valid throughput pilot read lower than the published runs: 13,102 to 66,533 events a second, against 151,897 to 179,224. The throughput pilots ran on earlier commits of the drivers, with fills of 30 or 90 event minutes against 240, and some on battery. One throughput pilot, tp-20261112T111814, recorded busier neighbours: other projects' containers used 69.1 → 154.8 percent of one CPU, above every published reading (12.7 to 65.8). No run measured which of these moved the rate.

- tp-20261112T100846: recorded from a dirty tree.
- tp-20261112T103845: not valid: run tp-20261112T103845-r1: the window is 12 s and 1 intervals; it needs 120 s and 8. Raise BENCH_MINUTES
- tp-20261112T110455: not valid: run tp-20261112T110455-r1: no sample lies between the warm-up and the last tenth of the fill; raise BENCH_MINUTES

## What these numbers do not show

All three measurements use synthetic data. They come from one laptop, with a single broker on the same VM and parallelism 2. Two things are read only when a run starts and when it ends: the shared VM's load, since other projects' containers run in it, and the power source. So the page does not show the load during a run, and the drivers do not see power between the two readings; the OS power log above shows it. Throughput was measured on a 240-minute fill, so every key was scored against the fallback baselines; the scorer was busy 11.7% of the window in tp-20261112T153247-r1 and 11.7% in tp-20261112T153247-r2. No sparse partition was measured. The delay run had no failover. A host sleep steps the VM's clock, and Flink then restarts the job; a run in which that happened is discarded.

## Reproduce

The data is synthetic and the generators are seeded. `make bench` runs the three targets and then renders this page:

- `make bench-throughput` fills a topic, then reads it twice with the live job.
- `make bench-delay` runs history, live traffic and injected incidents.
- `make bench-state` runs five weeks through the job on two state backends.
- `make power-log` reads the saved OS power log into `docs/performance/power.json`.
- `make performance` renders this page from `docs/performance/*.json`.

Run conditions: AC power, the lid open throughout, no other job on the cluster and the Flink web UI closed. `KEEP_AWAKE=1` wraps each driver in `caffeinate -is`. The report refuses a result recorded on battery, a result recorded from a dirty tree, a result marked invalid, a result that is not marked synthetic and a result whose own fields contradict its validity. It does not refuse the pilot runs, which are shown as they are.
