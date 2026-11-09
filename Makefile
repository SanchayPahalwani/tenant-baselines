MVN = docker run --rm -v $(PWD):/w -v tb-m2:/root/.m2 -w /w/flink-job maven:3.10.0-eclipse-temurin-17-noble mvn -B

.PHONY: jar test-java test-py lint-py up down reset

jar:
	$(MVN) clean package

test-java:
	$(MVN) clean verify

test-py:
	cd py && uv run pytest -q

lint-py:
	cd py && uv run ruff check . && uv run ruff format --check .

up:
	docker compose up -d --build && scripts/wait-ready.sh

down:
	docker compose down

reset:
	docker compose down -v
