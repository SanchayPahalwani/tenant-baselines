MVN = docker run --rm -v $(PWD):/w -v tb-m2:/root/.m2 -w /w/flink-job maven:3.10.0-eclipse-temurin-17-noble mvn -B

# `make ci` runs the workflow's commands in the same images, on a copy of the working tree,
# so the host's flink-job/target and py/.venv are left alone.
CI_COPY = mkdir /w && tar -C /src --exclude=.git --exclude=target --exclude=.venv \
	--exclude=__pycache__ --exclude=.pytest_cache --exclude=.ruff_cache -cf - . | tar -C /w -xf -
CI_PY_IMAGE = tenant-baselines-ci-py

# demo and smoke depend on their prerequisites running in order.
.NOTPARALLEL:

.PHONY: jar test-java test-py lint-py thresholds-demo check-am test ci up down reset submit demo smoke logs \
	eval-dev check-paths results eval bench-throughput

# The dev seed and amplitude of `make eval-dev`.
SEED ?= 7
AMP ?= 6

jar:
	$(MVN) clean package

test-java:
	$(MVN) clean verify

test-py:
	cd py && uv run pytest -q

lint-py:
	cd py && uv run ruff check . && uv run ruff format --check .

# Regenerates the demo's thresholds file (setting p99.9, fitted on three simulated weeks).
thresholds-demo:
	cd py && uv run python -m tenant_baselines.thresholds --demo --out ../infra/flink/thresholds-demo.json

check-am:
	scripts/check-am.sh

test: test-java test-py lint-py

ci:
	docker run --rm -v "$(CURDIR)":/src:ro -v tb-m2:/root/.m2 maven:3.10.0-eclipse-temurin-17-noble \
		sh -c '$(CI_COPY) && cd /w/flink-job && mvn -B verify'
	printf 'FROM python:3.12-slim\nCOPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /uvx /bin/\n' \
		| docker build -q -t $(CI_PY_IMAGE) - > /dev/null
	docker run --rm -v "$(CURDIR)":/src:ro -v tb-uv-cache:/root/.cache/uv $(CI_PY_IMAGE) \
		sh -c '$(CI_COPY) && cd /w/py && uv sync && uv run ruff check . \
		&& uv run ruff format --check . && uv run pytest -q'
	scripts/check-am.sh

# data/ is the jobmanager's read-only eval mount; created here so Docker does not create it.
up:
	mkdir -p data && docker compose up -d --build && scripts/wait-ready.sh

# The demo profile is included so the generator container goes too.
down:
	docker compose --profile demo down

reset:
	docker compose --profile demo down -v

# Replaces the running job with one restored from the newest retained checkpoint, if any.
submit:
	docker compose exec -T jobmanager bash /opt/tb-scripts/submit-job.sh

# Needs an empty stack (make reset): the demo timeline starts three weeks back and cannot
# restart on data that is already there. scripts/demo.sh refuses if live.events has data.
demo: jar up
	scripts/demo.sh

smoke: demo
	scripts/smoke.sh

logs:
	docker compose --profile demo logs -f --tail=100

# One dev run end to end (synthetic data): data/eval/<run_id>/summary.json.
eval-dev: jar up
	cd py && uv run python -m tenant_baselines.eval_run --kind dev --seed $(SEED) --amplitude $(AMP)

# The event-path equivalence check (spec 5.7, synthetic data): the paths world through the job as
# events and as aggregates; writes docs/results/check-paths.json.
check-paths: jar up
	cd py && uv run python -m tenant_baselines.check_paths

# Renders docs/results.md from docs/results/summaries (synthetic data).
results:
	cd py && uv run python -m tenant_baselines.eval_report --summaries ../docs/results/summaries --out ../docs/results.md

# The official runs (synthetic data), once per (seed, amplitude), then docs/results.md. The jar is
# the one built at the freeze, so `up` and not `jar`. Resumes after an interruption.
# `make eval` ran the 15 official runs. Each pair runs once, so it now only renders
# `docs/results.md`.
# A re-run goes through the driver itself, not `make eval` (step 3 below).
#
# Re-running after a bug fix (spec 5.3). A bug is code that disagrees with the spec; anything
# else is a setting change and may not be re-run.
#   1. Fix the bug and commit the fix.
#   2. If the Java code changed: `make jar`, copy flink-job/target/tenant-baselines-job.jar to
#      data/frozen/<its sha256>.jar, set `jar_sha256` in eval/freeze.json to that hash, and
#      commit. data/frozen/ is not in git: keep it, with the old copy beside the new one.
#   3. With the stack up (`make up`), re-run every pair, one invocation each (15 in all),
#      through the driver itself, for example
#      cd py && uv run python -m tenant_baselines.eval_official --only-seed 4101 \
#        --only-amplitude 6 --rerun-reason 'bug: <what>'
#      Not through `make eval`: its render step refuses summaries of two commits, so it would
#      fail after each of the first 14 re-runs although the run was recorded. Each re-run writes
#      a run-log line before it runs and moves the pair's old summary to
#      docs/results/superseded/ once it finishes.
#   4. `make results` renders docs/results.md. It refuses summaries of two commits, so every
#      pair must have been re-run.
# KEEP_AWAKE=1 wraps the driver in caffeinate; the default touches nothing about sleep.
KEEP_AWAKE ?= 0
ifeq ($(KEEP_AWAKE),1)
AWAKE = caffeinate -is
endif

eval: up
ifeq ($(KEEP_AWAKE),1)
	@echo "caffeinate -is: no idle sleep while this runs, and no system sleep on AC power. It does not stop the sleep when the lid closes. Keep the lid open."
endif
	cd py && $(AWAKE) uv run python -m tenant_baselines.eval_official $(EVAL_ARGS) \
		&& uv run python -m tenant_baselines.eval_report --summaries ../docs/results/summaries --out ../docs/results.md

BENCH_MINUTES ?= 60
# The throughput benchmark (synthetic data): pre-fills a topic, then reads it twice with the
# live job. Needs AC power, no other job on the cluster, and the Flink web UI closed.
bench-throughput: jar up
	cd py && $(AWAKE) uv run python -m tenant_baselines.bench_throughput \
		--minutes $(BENCH_MINUTES) $(BENCH_ARGS)
