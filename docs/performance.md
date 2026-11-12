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
| Images | apache/kafka:4.4.0, apache/kafka:4.4.0, clickhouse/clickhouse-server:26.8.36.3, flink:2.2.1-java17, flink:2.2.1-java17, flink:2.2.1-java17, grafana/grafana:13.2.5, prom/alertmanager:v0.34.2, tenant-baselines-forwarder, tenant-baselines-pager-sink | apache/kafka:4.4.0, apache/kafka:4.4.0, clickhouse/clickhouse-server:26.8.36.3, flink:2.2.1-java17, flink:2.2.1-java17, flink:2.2.1-java17, grafana/grafana:13.2.5, prom/alertmanager:v0.34.2, tenant-baselines-forwarder, tenant-baselines-pager-sink | apache/kafka:4.4.0, apache/kafka:4.4.0, clickhouse/clickhouse-server:26.8.36.3, flink:2.2.1-java17, flink:2.2.1-java17, flink:2.2.1-java17, grafana/grafana:13.2.5, prom/alertmanager:v0.34.2, tenant-baselines-forwarder, tenant-baselines-pager-sink |
| Flink version | 2.2.1 | 2.2.1 | 2.2.1 |
| Parallelism | 2 | 2 | 2 |
| Task slots | 4 | 4 | 4 |
| TaskManager memory | 2560m | 2560m | 2560m |
| Power | Now drawing from 'AC Power' | Now drawing from 'AC Power' | Now drawing from 'AC Power' |

The state runs' backend and checkpoint interval are in the state-size section, not in this table.

## Throughput

This section uses synthetic data. The fill had 2,000 keys at 120 requests a minute each, for 240 event minutes: 57,590,352 events in 8 partitions. The generator filled the topic at 682,318 events a second (84.4 s), so the job's rate can be set beside the rate at which the same events were written.

The topic was complete before the job started, and nothing produced during the run. The rate is the rise of the source's record counter over a window that leaves out the first 60 s after the counter moved and the last 10% of the fill, sampled every 12 s.

| Run | Sustained events a second | Source's own clock, events a second | Window (s) | Lowest to highest interval rate | Source back-pressured | Source idle | Stale / re-read samples |
| --- | --- | --- | --- | --- | --- | --- | --- |
| tp-20261112T153247-r1 | 179,224 | 179,221 | 192.0 | 132,569.6 to 193,694.4 | 87.5% | 0.2% | 0 / 0 |
| tp-20261112T153247-r2 | 151,897 | 151,829 | 240.0 | 106,655.3 to 193,124.3 | 87.2% | 0.2% | 0 / 0 |

The two runs differ by 16.5% of their mean. The first job on a TaskManager starts colder; tp-20261112T153247-r1 was the faster run here. The source's own clock is a cross-check that does not depend on the host's timing. A source that is neither busy nor back-pressured would be waiting on the broker; the idle share above shows whether that happened.

`timing_error_pct` is a bound, computed from the longest measured read delay at the window's two ends over the window: tp-20261112T153247-r1: 1.6%; tp-20261112T153247-r2: 1.3%.

The busiest vertex: tp-20261112T153247-r1: minute-aggregator -> (aggregates-sink: Writer -> aggregates-sink: Committer, late-events -> late-events-discard: Writer) at 98.7%; tp-20261112T153247-r2: minute-aggregator -> (aggregates-sink: Writer -> aggregates-sink: Committer, late-events -> late-events-discard: Writer) at 98.8%.

The `drop-ticks` vertex's busy share, as a share of parallelism times the window, is tp-20261112T153247-r1: 6.0%; tp-20261112T153247-r2: 6.0%. It is reported as a share of CPU time, not as a throughput difference. The drop-ticks task's busy time also covers handing each event on to the keyed shuffle, work the source task would do itself without the union, so this share is an upper bound on the extra hop's cost.

The job ran 3 settings at once. The alert rows each run wrote, counted from `bench.alerts`:

No run wrote an alert row: the bench world stays under every fire level.

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

The baselines are cold after the fill's event time, and no unit/tenant pair goes stale in this world, so these sizes are not the state of a warmed-up job.

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

Pipeline delay is the time from a row's event time to the broker's append time of that row in `bench.alerts`. Time-to-page is the time from the incident's start to the append of the first matching row. Percentiles are nearest-rank, with their `n`. Times are in seconds. The headline setting is seasonal/k4.

| Detector / setting | Incidents | Matched | Pipeline delay p50 | Pipeline delay p95 | Time to page p50 | Time to page p95 | Time to page minimum |
| --- | --- | --- | --- | --- | --- | --- | --- |
| global_static/p99.9 | 60 | 60 | 21.6 (n=60) | 22.5 (n=60) | 309.4 (n=60) | 364.4 (n=60) | 269.4 (n=60) |
| per_key_static/p99.9 | 60 | 60 | 21.6 (n=60) | 22.5 (n=60) | 309.4 (n=60) | 364.4 (n=60) | 269.4 (n=60) |
| seasonal/k4 (headline) | 60 | 60 | 21.6 (n=60) | 22.5 (n=60) | 296.7 (n=60) | 324.4 (n=60) | 269.4 (n=60) |

How time-to-page is built (configured, not measured): the detector fires after 3 scored minutes in a row, and the alert is then held for 120 s. Both are counted from the start of the first breaching minute, not from the incident's start. A spike of this size is expected to breach in the minute it starts in, so the count often starts up to a minute before the incident. The measured minimum is 269.4 s, and 35 of 60 matched incidents were paged in under five minutes from their start.

The watermark lag, measured during the run and corrected by the VM clock's offset (before 0.068 s, after 0.096 s): minimum 21.3 s, median 21.8 s, maximum 23.1 s over 68 samples. By design (configured, not measured), the lag is the 20 s watermark bound plus the age of the newest event on the slowest partition that is not idle. This benchmark sends at a fixed rate, so every partition is busy. A sparse partition adds up to 60 s (the idleness timeout, configured); that case was not measured here.

Both figures end at the append to the alerts topic. The forwarder and Alertmanager's `group_wait` (30 s, configured) come after it. The README's milestone 3 page times were measured at the webhook, from the incident's start minute, and are not comparable.

The split of the headline pipeline delay, in seconds: decision to emit 21.6 at p50 and 22.5 at p95; emit to append 0.0 at p50 and 0.0 at p95.

For the headline setting: 0 incidents were missed, 0 firing rows matched no incident, and 0 regional firing rows were written.

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
The figure moves because SST files that are not yet compacted are counted.

This job ran against 12 settings; the live job runs 3. The baseline history, which all settings share, is most of a key's state.

## Extrapolation, not measured

This section is an extrapolation from synthetic measurements, not a measurement. Bytes are the scorer's bytes per key times the key count.

| Keys | hashmap (bytes) | rocksdb (bytes) |
| --- | --- | --- |
| 2,000 | 1,244,476,167 | 573,562,083 |
| 20,000 | 12,444,761,667 | 5,735,620,833 |
| 100,000 | 62,223,808,333 | 28,678,104,167 |

It assumes every key has a full 5-week history and that state grows linearly with keys. Neither was tested above the measured key count. It leaves out the region grouper's per-key activity, the minute aggregator's state in events mode and the population baselines.

## What these numbers do not show

All three measurements use synthetic data. They come from one laptop, with a single broker on the same VM and parallelism 2. Throughput was measured with cold baselines. No sparse partition was measured. The delay run had no failover. A host sleep steps the VM's clock, and Flink then restarts the job; a run in which that happened is discarded.

## Reproduce

The data is synthetic and the generators are seeded. `make bench` runs the three targets and then renders this page:

- `make bench-throughput` fills a topic, then reads it twice with the live job.
- `make bench-delay` runs history, live traffic and injected incidents.
- `make bench-state` runs five weeks through the job on two state backends.
- `make performance` renders this page from `docs/performance/*.json`.

Run conditions: AC power, the lid open throughout, no other job on the cluster and the Flink web UI closed. `KEEP_AWAKE=1` wraps each driver in `caffeinate -is`. The report refuses a result recorded on battery, a result marked invalid and a result that is not marked synthetic.
