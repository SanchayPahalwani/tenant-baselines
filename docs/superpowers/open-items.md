# Open items

Planning material; nothing in it blocked a milestone. Each line is a follow-up that milestone 6 did not do, with the reason. The decisions are in the milestone 6 plan's follow-up table (`plans/2026-11-13-m6-readme-design.md`). The sources are `m1-followups.md`, `m2-followups.md` and `m3-followups.md`.

## Job code

- `PopulationBaselines` keeps stale week-4 histogram entries when a bucket's hour gets no scored minute for a week. A sweep of weeks below `w - 4` changes the job and the jar, which the sealed eval forbids. (m2; design doc section 12 names it.)
- `unit-tenants` state removes stale unit/tenant pairs only lazily. This is job code. No bench world makes a pair stale, so it is not measured. (m3; design doc section 12 names it.)
- Unit status is separable from `RegionGrouper.java` (about 670 lines). Job code; extract it when the file is next touched.
- Java minors in `ThresholdsFile`, `AggregateJson` and `Scorer.onTimer` (a non-object node, a non-integer `valid_from_ms`, the overlap message, one long line, an unbounded error message, an untested `fin && due > replayEndMs` branch). Job code; they change the jar.
- A test that a drained unit keeps its first signal and reason until it undrains. The behaviour is stated in the design doc, from the code. No Java test is added in this milestone.
- Connector 5.1.0. Stays open until it is released; the design doc states the rule for a replay-mode run that hits FLINK-39728.
- The shaded jar keeps merged NOTICE files but drops bundled dependencies' own LICENSE files. A fix edits `pom.xml` and changes the jar. The jar is not published.
- The live job's retained checkpoints are not deleted. `submit-job.sh` restores from them, and a change needs `make smoke`. `make reset` removes them.

## Compose and infra (measured paths)

- Alertmanager state is on an anonymous volume, so silences do not survive `make down`. Edits `docker-compose.yml`, a measured path.
- `tb.ingest_errors` has no TTL. Edits a measured `infra/` file.
- The `live.*` topics are stamped with event time, so the 7-day retention can delete backfilled rows. `LogAppendTime` would edit `infra/kafka`, a measured path. The design doc states the effect.
- The Python image runs as root. A non-root user needs the `pager-data` volume re-owned, which is a compose change.
- `create-topics.sh --if-not-exists` keeps an old stack's topic configs. Measured path.

## Eval driver and report

- A reader cannot reproduce an official run. A `repro` kind (an official seed, a local build, output under `data/eval/repro-*`, a comparison of `selected_week5` with the committed summary) is not built. The README says a reader can run dev seeds with `make eval-dev`. Reason: it needs a new driver kind and a build kept apart from the frozen jar; no published figure depends on it.
- No false-page breakdown script (bursts, day factors, the rest). The README keeps "this eval does not measure the split". Reason: it is new analysis, not a fix; the eval's figures stand without it.
- After a bug re-run, the "measured code equals the freeze" line still compares HEAD with the freeze commit, and any `bug:` entry accepts a "no". Measured Python module; any edit breaks the freeze line.
- After a bug fix, a failed re-run can only be repeated under `bug:`, not `infrastructure:`. Same reason.
- With a selection that matches no planned pair, the driver prints "all 0 official runs are finished". Same reason.
- A dev render prints the official runs' machine-facts source line. Same reason.
- `eval_report.py` defines its own `REPO`; it should import it from `freeze`. Same reason.
- The test resource `thresholds-eval-dev-s7-a6.json` cannot carry a "synthetic" label, because the format has no field for one.
- `flink-job/pom.xml` is not in `freeze.MEASURED_PATHS`. An official run's frozen jar hash covers it, and `test_docs.py` checks it against the freeze commit. Reason: adding it edits the guard again after the official runs. (M6 C2.)
- `eval_run`'s poll retries can overshoot `timeout_s` by about a minute. Reason: the eval driver stays as the official runs used it; the overshoot only lengthens a run that is failing anyway.

## Scripts and tests

- The default `submit-job.sh` misses jobs in CREATED or INITIALIZING. `make demo` submits once on an empty cluster. A change needs smoke checks 11 and 12 read again.
- `submit-job.sh` writes the checkpoint path twice. Reason: both values are the same path, so it has no effect; it waits for the next change to that script.
- The forwarder posts all due alerts in one request, so one rejected entry hides which one failed. The body log is not added; no reader sees it.
- `smoke.sh`'s `check_inhibit` prints a Python traceback on every poll while Alertmanager is down. Reason: it is noise only; the check still passes or fails correctly once Alertmanager answers.
- The demo generator's delivery timeout is 300 s; only `--script bench` has the shorter one. A change to the live producer's settings wants its own smoke run.
- `watermark-lag.sh` has no offset correction. Its header says the lag is against the host's clock. Reason: it is a quick manual check; the published lag comes from `bench_flink.watermark_lag_sample`, which subtracts the measured offset.
- The benchmark driver reads power only at its start and end. It should sample `pmset -g ps` at every poll, record the changes in the result and decide the rule (refuse the run, or print the change). Changing the driver is not worth it without a re-run, and no benchmark is re-run. `docs/performance.md` prints the stopgap from `docs/performance/power.json`. (M5 C1.)
- The result files record a start but no end. `power.json` takes the end from the write time of `data/bench/<run_id>/result.json`, which is not in git. A fix changes the driver and the result format. (M5 C2.)
- Shared private helpers (`bench_throughput._load`, `_settings`, `_wait_running`, `bench_flink._json`, `bench_env._sh`, `_power_line`) should move into `bench_env` and `bench_flink`. A refactor with no behaviour change; left alone. (M5 C3.)
- One read-twice helper for `sample`, `late_events` and `watermark_lag_sample`. Same reason. (M5 C4.)
- Two restart edges that end as `RunFailed`: a restart before the first checkpoint, and one after the last sample. They need a run to see. (M5 C5.)
- Load as more than two instants: sample `docker stats` during a throughput run. It needs a new throughput run. (M5 C7.)
- The stale-read path has not been seen live under the current code. Needs a new delay run, or an old pilot's samples kept as a test fixture. (M5 C8.)
- A warmed-up throughput run (history first, then the dense fill), so the cold-baseline caveat becomes a figure. Needs a new run. (M5 C9.)
- Two test nits from milestone 5 Task 6: `test_measure_run_fails_when_no_checkpoint_is_in_the_last_share` matches a message both failures contain, and `measure_run` reads `/checkpoints` and `/checkpoints/config` twice each. Reason: test and efficiency nits only; neither changes a result.

## CI and the dashboard

- The GitHub Actions workflow has not run on GitHub. Unproven until the owner's first push: the action tags (`checkout@v7`, `setup-java@v6`, `setup-uv@v10.3.3`), the runner's Maven, and MiniCluster timing on a smaller runner. Reason: only the owner pushes; `make ci` runs the same commands locally. (M6 C1.)
- `test_ci_sync.py` does not catch a command that only `make ci` runs. Reason: such a command makes `make ci` stricter than the workflow, never looser, and the recipe also holds setup lines (the copy of the tree, the image build) that a stricter test would have to list. (M6 C3.)
- How the panels render was not checked in a browser. Smoke check 13 runs every panel's query through Grafana's query API, and the design doc says the rendering was not checked. Reason: it needs the stack and a person looking at the page. (M6 C4.)
