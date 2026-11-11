# tenant-baselines

A small reference pipeline for anomaly alerting on multi-tenant service traffic, sized to run on one laptop. Request events go through Kafka into a Flink job, which builds per-minute aggregates for each tenant, region and unit, checks them against thresholds and writes alerts back to Kafka. ClickHouse and Grafana show the aggregates and the alert log, and Prometheus Alertmanager routes the alerts. The question the project is built to test is whether a baseline per key and hour of the week catches more incidents and pages less than fixed thresholds when traffic follows working hours.

Work in progress: milestone 3 of 6. A seasonal detector scores each key's minute against the median and spread of the same hour of the week over the key's last four weeks, falling back to the region's and then the global baseline for error rate and latency while a key is new; it pages through Alertmanager, and the static thresholds run beside it. Every tenant alert is held for 2 minutes. If at least half of a region's active tenants (at least three) alert on the same signal when the hold ends, one regional alert pages instead and absorbs the tenant alerts; a dedicated unit is drained while its tenant alerts alone and undrained when the region alerts, and `tb.unit_status` records both. There are no eval results yet.

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

The generator writes three weeks of synthetic history at full speed and then continues live, so `make demo` needs an empty stack: on one that already has events it refuses and asks for `make reset`. Warm-up, until the job has caught up with the wall clock, took 235 s in one clean run from empty volumes (the generator caught up after 119 s and 24,338,688 events), not counting the first image build. Every figure in this section comes from that run, on an arm64 laptop on AC power with a Colima Docker VM of 8 CPUs and 10 GB, on Wednesday at 18:33 UTC. The live watermark lag measured on it (`scripts/watermark-lag.sh 60 11`, 60 samples 11 s apart, from Wednesday 18:37 UTC) was 21.1 s at the minimum, 22.5 s at the median and 25.7 s at the maximum. From the start minute of each scripted incident to its page, `pager-sink` logged 388.6 s for the tenant drop, 389.5 s for the error burst and 326.6 s for the regional error spike. These times include the 2-minute hold and the watermark lag, which is between 21 s and 80 s depending on how busy the quietest partition is; the run above had every partition busy. They are not comparable with milestone 2's page times, which had no hold. A total outage across a job restore (`make smoke` check 12) paged a `volume_5m` alert 310 s after the generator stopped; two earlier runs took 368 s and 565 s. At catch-up the TaskManager used 1.396 GiB of its 2.5 GiB limit, the latest checkpoint was 6.4 MiB, and the job had dropped no events as late (`lateEvents` 0). The demo has three tenants with a flat synthetic profile, so a regional alert can form at any hour. Keep the machine awake during the demo: a sleep freezes the Docker VM, the job sees it as a total outage, and on wake the zero-traffic rule pages every busy key. `make demo` then prints the Grafana, Alertmanager and Flink URLs, the times of the three scripted incidents, and what to watch for.

`make smoke` runs the demo and checks it end to end (run `make reset` first), `make test` runs the unit tests, and `make ci` runs the GitHub Actions workflow's commands locally. `make down` stops the stack; `make reset` also deletes its data.

The ClickHouse user `tb` with password `tb-local` is a local demo credential, not a secret.

## Design

The design and its reasons are in [docs/superpowers/specs/2026-11-09-tenant-baselines-design.md](docs/superpowers/specs/2026-11-09-tenant-baselines-design.md), the milestone 1 plan is in [docs/superpowers/plans/2026-11-09-m1-static-end-to-end.md](docs/superpowers/plans/2026-11-09-m1-static-end-to-end.md), the milestone 2 plan is in [docs/superpowers/plans/2026-11-10-m2-seasonal-baselines.md](docs/superpowers/plans/2026-11-10-m2-seasonal-baselines.md), and the milestone 3 plan is in [docs/superpowers/plans/2026-11-11-m3-alert-noise-control.md](docs/superpowers/plans/2026-11-11-m3-alert-noise-control.md).

Licensed under Apache-2.0.
