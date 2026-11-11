# tenant-baselines

A small reference pipeline for anomaly alerting on multi-tenant service traffic, sized to run on one laptop. Request events go through Kafka into a Flink job, which builds per-minute aggregates for each tenant, region and unit, checks them against thresholds and writes alerts back to Kafka. ClickHouse and Grafana show the aggregates and the alert log, and Prometheus Alertmanager routes the alerts. The question the project is built to test is whether a baseline per key and hour of the week catches more incidents and pages less than fixed thresholds when traffic follows working hours.

Work in progress: milestone 2 of 6. A seasonal detector scores each key's minute against the median and spread of the same hour of the week over the key's last four weeks, falling back to the region's and then the global baseline for error rate and latency while a key is new; it pages through Alertmanager, and the static thresholds run beside it. There are no eval results yet.

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

The generator writes three weeks of synthetic history at full speed and then continues live, so `make demo` needs an empty stack: on one that already has events it refuses and asks for `make reset`. Warm-up, until the job has caught up with the wall clock, took 167 s and 213 s in two runs on an arm64 laptop with an 8-CPU, 10 GB Docker VM, not counting the first image build. Keep the machine awake during the demo: a sleep freezes the Docker VM, the job sees it as a total outage, and on wake the zero-traffic rule pages every busy key. `make demo` then prints the Grafana, Alertmanager and Flink URLs, the times of the scripted tenant drop and regional error spike, and what to watch for.

`make smoke` runs the demo and checks it end to end (run `make reset` first), `make test` runs the unit tests, and `make ci` runs the GitHub Actions workflow's commands locally. `make down` stops the stack; `make reset` also deletes its data.

The ClickHouse user `tb` with password `tb-local` is a local demo credential, not a secret.

## Design

The design and its reasons are in [docs/superpowers/specs/2026-11-09-tenant-baselines-design.md](docs/superpowers/specs/2026-11-09-tenant-baselines-design.md), the milestone 1 plan is in [docs/superpowers/plans/2026-11-09-m1-static-end-to-end.md](docs/superpowers/plans/2026-11-09-m1-static-end-to-end.md), and the milestone 2 plan is in [docs/superpowers/plans/2026-11-10-m2-seasonal-baselines.md](docs/superpowers/plans/2026-11-10-m2-seasonal-baselines.md).

Licensed under Apache-2.0.
