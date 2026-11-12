# tenant-baselines

A small reference pipeline for anomaly alerting on multi-tenant service traffic, sized to run on one laptop. Request events go through Kafka into a Flink job, which builds per-minute aggregates for each tenant, region and unit, checks them against thresholds and writes alerts back to Kafka. ClickHouse and Grafana show the aggregates and the alert log, and Prometheus Alertmanager routes the alerts. The question the project is built to test is whether a baseline per key and hour of the week catches more incidents and pages less than fixed thresholds when traffic follows working hours.

Work in progress: milestone 5 of 6. A seasonal detector scores each key's minute against the median and spread of the same hour of the week over the key's last four weeks, falling back to the region's and then the global baseline for error rate and latency while a key is new; it pages through Alertmanager, and the static thresholds run beside it. Every tenant alert is held for 2 minutes. If at least half of a region's active tenants (at least three) alert on the same signal when the hold ends, one regional alert forms and usually absorbs the other tenants' alerts; a tenant alert sent before it formed has already paged, and in the measured run below the burst's tenant page (18:44 UTC) went out before the regional alert (18:51 UTC). Unit status is only published to `tb.unit_status`; nothing acts on it. Drains cover error rate and latency, never volume: a dedicated unit drains on one tenant's sent alert, a shared unit when two tenants alert on the same signal, and no unit is drained during a regional alert. Milestone 4 adds the detection eval, described under Eval below. Milestone 5 adds the throughput, alert delay and state size measurements, described in [docs/performance.md](docs/performance.md).

**Synthetic data only.** Every event comes from the generator in `py/`; none of it describes a real system.

## Prerequisites

- Docker with at least 8 CPUs and 10 GB of memory
- `make` and [`uv`](https://docs.astral.sh/uv/)

The compose network uses the fixed subnet 10.231.0.0/24; change it in `docker-compose.yml` if it clashes with a local network. Published ports listen on 127.0.0.1 only.

## Quickstart

```sh
make reset   # make demo needs an empty stack
make demo    # build the job, start the stack and the generator, submit the job, wait for warm-up
```

The generator writes three weeks of synthetic history at full speed and then continues live, so `make demo` needs an empty stack: on one that already has events it refuses and asks for `make reset`. Warm-up, until the job has caught up with the wall clock, took 235 s in one clean run from empty volumes (the generator caught up after 119 s and 24,338,688 events), not counting the first image build.

The figures below come from that one run, except where marked as earlier runs. It ran on an arm64 laptop on AC power with a Colima Docker VM of 8 CPUs and 10 GB, on Wednesday 2026-11-11 at 18:33 UTC. The live watermark lag measured on it (`scripts/watermark-lag.sh 60 11`, 60 samples 11 s apart, from Wednesday 18:37 UTC) was 21.1 s at the minimum, 22.5 s at the median and 25.7 s at the maximum. From the start minute of each scripted incident to its page, `pager-sink` logged 388.6 s for the tenant drop, 389.5 s for the error burst and 326.6 s for the regional error spike. These times include the 2-minute hold and the watermark lag. By design the lag is bounded by 20 s plus up to 60 s of partition idleness, so between 21 s and 80 s depending on how busy the quietest partition is; the lag measured here was 21.1 s to 25.7 s. The times are not comparable with milestone 2's page times, which had no hold. A total outage across a job restore (`make smoke` check 12) raised a `volume_5m` alert 310 s after the generator stopped; two earlier runs on the same machine took 368 s and 565 s. At catch-up the TaskManager used 1.396 GiB of its 2.5 GiB limit, the latest checkpoint was 6.4 MiB, and the job had dropped no events as late (`lateEvents` 0). The demo has three tenants with a flat synthetic profile, so a regional alert can form at any hour.

Keep the machine awake during the demo: a sleep freezes the Docker VM, the job sees it as a total outage, and on wake the zero-traffic rule pages every busy key. `make demo` then prints the Grafana, Alertmanager and Flink URLs, the times of the three scripted incidents, and what to watch for.

`make smoke` runs the demo and checks it end to end (run `make reset` first), `make test` runs the unit tests, and `make ci` runs the GitHub Actions workflow's commands locally. `make down` stops the stack; `make reset` also deletes its data.

The ClickHouse user `tb` with password `tb-local` is a local demo credential, not a secret.

## Eval

The eval compares three detectors on labelled, synthetic replays: the seasonal baseline, a static threshold per key, and one global static threshold per signal. Each has four settings, and each seed picks its own setting on week 4 of a five-week replay. The result below is that setting on week 5. Five seeds run at three day/night amplitudes (1.5:1, 6:1 and 12:1). Recall counts a scenario when a page lands on the signal the scenario moves. The scenario table was fixed before any run.

**What the result shows.** All traffic is synthetic, and it follows working hours because the generator says so. The eval tests whether this implementation delivers the benefit when that assumption holds, and how the benefit changes as seasonality weakens. It is not evidence about any real fleet, and it makes no claim of production use.

At the main amplitude (6:1), pooled over the 5 seeds (copied from [docs/results.md](docs/results.md); `py/tests/test_docs.py` checks the copy and every figure below against the records):

| Family | Expected-signal recall, pooled k/n (interval) | False pages per day, pooled |
|---|---|---|
| seasonal | 45/50 (0.79–0.96) | 9.63 |
| per key static | 38/50 (0.63–0.86) | 21.20 |
| global static | 31/50 (0.48–0.74) | 12.69 |

At 6:1 the seasonal detector found more scenarios than either static detector. Pooled over the seeds it had fewer false pages per day than both (9.63 against 21.20 and 12.69). Per seed, the global static range (5.29–17.71) overlaps the seasonal range (7.86–11.14), so the gap to global static is not settled with 5 seeds. The recall intervals of seasonal and per key static overlap, so with 5 seeds that recall gap is not settled. Most pages are false in every family: at the selected settings, 73 of 410 pages matched a scenario (seasonal), 60 of 802 (per key static) and 51 of 495 (global static). The traffic model's unlabelled bursts and per-day level factors (spec 5.2) are the expected cause; this eval does not measure the split. At 1.5:1 the seasonal detector found every scenario (50/50), but the global static detector had fewer false pages per day (12.46 against 13.63, pooled). The full tables, the sweep, every setting and the per-scenario results are in [docs/results.md](docs/results.md).

`make eval` ran the 15 official runs. Each pair runs once, so it now only renders `docs/results.md`. The runs took 40.5 minutes on this machine (an Apple M3 Pro laptop), from the first run's start to the last run's end in `docs/results/official-runs.jsonl`. A re-run after a bug fix (spec 5.3) follows the steps in the comment above `eval` in the `Makefile`. `make check-paths` sends one synthetic world through the job as events and as pre-built minute aggregates. At commit `e57f621`, with a jar built from the same source as the frozen one (`flink-job/pom.xml`, `flink-job/src/main` and `shared/` are unchanged since 9f25a23), both alert logs and both sets of aggregates were identical ([docs/results/check-paths.json](docs/results/check-paths.json)). The check took 46.5 minutes on this machine. It checks that the two paths agree, not that the detectors are right.

The plan for this milestone is [docs/superpowers/plans/2026-11-12-m5-throughput-delay.md](docs/superpowers/plans/2026-11-12-m5-throughput-delay.md).

## Design

The design and its reasons are in [docs/superpowers/specs/2026-11-09-tenant-baselines-design.md](docs/superpowers/specs/2026-11-09-tenant-baselines-design.md), the milestone 1 plan is in [docs/superpowers/plans/2026-11-09-m1-static-end-to-end.md](docs/superpowers/plans/2026-11-09-m1-static-end-to-end.md), the milestone 2 plan is in [docs/superpowers/plans/2026-11-10-m2-seasonal-baselines.md](docs/superpowers/plans/2026-11-10-m2-seasonal-baselines.md), the milestone 3 plan is in [docs/superpowers/plans/2026-11-11-m3-alert-noise-control.md](docs/superpowers/plans/2026-11-11-m3-alert-noise-control.md), the milestone 4 plan is in [docs/superpowers/plans/2026-11-12-m4-eval.md](docs/superpowers/plans/2026-11-12-m4-eval.md), and the milestone 5 plan is in [docs/superpowers/plans/2026-11-12-m5-throughput-delay.md](docs/superpowers/plans/2026-11-12-m5-throughput-delay.md). Throughput, alert delay and state size, measured on one laptop on synthetic data, are in [docs/performance.md](docs/performance.md).

Licensed under Apache-2.0.
