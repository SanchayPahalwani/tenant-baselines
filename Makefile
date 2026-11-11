MVN = docker run --rm -v $(PWD):/w -v tb-m2:/root/.m2 -w /w/flink-job maven:3.10.0-eclipse-temurin-17-noble mvn -B

# `make ci` runs the workflow's commands in the same images, on a copy of the working tree,
# so the host's flink-job/target and py/.venv are left alone.
CI_COPY = mkdir /w && tar -C /src --exclude=.git --exclude=target --exclude=.venv \
	--exclude=__pycache__ --exclude=.pytest_cache --exclude=.ruff_cache -cf - . | tar -C /w -xf -
CI_PY_IMAGE = tenant-baselines-ci-py

# demo and smoke depend on their prerequisites running in order.
.NOTPARALLEL:

.PHONY: jar test-java test-py lint-py thresholds-demo check-am test ci up down reset submit demo smoke logs

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

up:
	docker compose up -d --build && scripts/wait-ready.sh

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
