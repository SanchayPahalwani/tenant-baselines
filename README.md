# tenant-baselines

A small reference pipeline for anomaly alerting on multi-tenant service traffic, sized to run on one laptop. Request events go through Kafka into a Flink job, which builds per-minute aggregates for each tenant, region and unit, checks them against thresholds and writes alerts back to Kafka. ClickHouse and Grafana show the aggregates and the alert log, and Prometheus Alertmanager routes the alerts. The question the project is built to test is whether a baseline per key and hour of the week catches more incidents and pages less than fixed thresholds when traffic follows working hours.

Work in progress: milestone 1 of 6. This milestone runs end to end with static thresholds only. There are no baselines and no eval results yet.

**Synthetic data only.** Every event comes from the generator in `py/`; none of it describes a real system.

## Prerequisites

- Docker with at least 8 CPUs and 10 GB of memory
- `make` and [`uv`](https://docs.astral.sh/uv/)

The compose network uses the fixed subnet 10.231.0.0/24; change it in `docker-compose.yml` if it clashes with a local network. Published ports listen on 127.0.0.1 only.

## Quickstart

```sh
docker compose up -d   # start the stack
make demo              # build and submit the job, start the generator, print the URLs
```

`make demo` prints the Grafana, Alertmanager and Flink URLs and what to watch for. `make smoke` runs the demo and checks it end to end (run `make reset` first), `make test` runs the unit tests, and `make ci` runs the GitHub Actions workflow's commands locally. `make down` stops the stack; `make reset` also deletes its data.

The ClickHouse user `tb` with password `tb-local` is a local demo credential, not a secret.

## Design

The design and its reasons are in [docs/superpowers/specs/2026-11-09-tenant-baselines-design.md](docs/superpowers/specs/2026-11-09-tenant-baselines-design.md), and the milestone 1 plan is in [docs/superpowers/plans/2026-11-09-m1-static-end-to-end.md](docs/superpowers/plans/2026-11-09-m1-static-end-to-end.md).

Licensed under Apache-2.0.
