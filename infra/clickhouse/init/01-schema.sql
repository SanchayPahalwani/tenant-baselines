-- ClickHouse pulls the job's output from Kafka. Each Kafka-engine table reads one
-- topic per namespace; materialized views copy good rows into ReplacingMergeTree
-- tables (deduplicated by the ORDER BY key, so query them with FINAL) and rows that
-- failed to parse into tb.ingest_errors. Runs once, on first start with an empty
-- data volume.

CREATE DATABASE IF NOT EXISTS tb;

-- Per-key minute aggregates (MinuteAgg JSON).
CREATE TABLE IF NOT EXISTS tb.kafka_aggregates
(
    run_id String,
    key String,
    tenant String,
    region String,
    unit String,
    minute_ms Int64,
    count UInt64,
    errors UInt64,
    p95_latency_ms Nullable(Float64)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'live.aggregates,eval.aggregates,bench.aggregates',
    kafka_group_name = 'ch-aggregates',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

-- Each signal of each key and minute against its seasonal baseline (ScoreEvent JSON).
-- The job writes these only with --scores true, the default in the live namespace. A
-- number that does not exist is null.
CREATE TABLE IF NOT EXISTS tb.kafka_scores
(
    run_id String,
    key String,
    tenant String,
    region String,
    unit String,
    signal String,
    minute_ms Int64,
    value Nullable(Float64),
    median Nullable(Float64),
    spread Nullable(Float64),
    score Nullable(Float64),
    source Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'live.scores,eval.scores,bench.scores',
    kafka_group_name = 'ch-scores',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

-- Alert transitions (AlertEvent JSON).
CREATE TABLE IF NOT EXISTS tb.kafka_alerts
(
    alert_id String,
    run_id String,
    detector String,
    setting String,
    scope String,
    key String,
    tenant String,
    region String,
    unit String,
    signal String,
    state String,
    starts_at_ms Int64,
    event_time_ms Int64,
    emitted_at_ms Int64,
    value Float64,
    reference Float64,
    reason Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'live.alerts,eval.alerts,bench.alerts',
    kafka_group_name = 'ch-alerts',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

-- Unit drains and undrains (UnitStatusEvent JSON). The topic is compacted, keyed by unit.
CREATE TABLE IF NOT EXISTS tb.kafka_unit_status
(
    run_id String,
    unit String,
    region String,
    status String,
    signal String,
    at_ms Int64,
    emitted_at_ms Int64,
    reason String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'live.unit-status,eval.unit-status,bench.unit-status',
    kafka_group_name = 'ch-unit-status',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS tb.aggregates
(
    ns LowCardinality(String),
    run_id LowCardinality(String),
    key LowCardinality(String),
    tenant LowCardinality(String),
    region LowCardinality(String),
    unit LowCardinality(String),
    minute DateTime64(3, 'UTC'),
    count UInt64,
    errors UInt64,
    p95_latency_ms Nullable(Float64)
)
ENGINE = ReplacingMergeTree
ORDER BY (ns, run_id, key, minute);

CREATE TABLE IF NOT EXISTS tb.scores
(
    ns LowCardinality(String),
    run_id LowCardinality(String),
    key LowCardinality(String),
    tenant LowCardinality(String),
    region LowCardinality(String),
    unit LowCardinality(String),
    signal LowCardinality(String),
    minute DateTime64(3, 'UTC'),
    value Nullable(Float64),
    median Nullable(Float64),
    spread Nullable(Float64),
    score Nullable(Float64),
    source LowCardinality(Nullable(String))
)
ENGINE = ReplacingMergeTree
ORDER BY (ns, run_id, key, signal, minute);

CREATE TABLE IF NOT EXISTS tb.alert_log
(
    ns LowCardinality(String),
    alert_id String,
    run_id LowCardinality(String),
    detector LowCardinality(String),
    setting LowCardinality(String),
    scope LowCardinality(String),
    key LowCardinality(String),
    tenant LowCardinality(String),
    region LowCardinality(String),
    unit LowCardinality(String),
    signal LowCardinality(String),
    state LowCardinality(String),
    starts_at DateTime64(3, 'UTC'),
    event_time DateTime64(3, 'UTC'),
    emitted_at DateTime64(3, 'UTC'),
    value Float64,
    reference Float64,
    reason Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (ns, run_id, alert_id, state);

CREATE TABLE IF NOT EXISTS tb.unit_status
(
    ns LowCardinality(String),
    run_id LowCardinality(String),
    unit LowCardinality(String),
    region LowCardinality(String),
    status LowCardinality(String),
    signal LowCardinality(String),
    at DateTime64(3, 'UTC'),
    emitted_at DateTime64(3, 'UTC'),
    reason LowCardinality(String)
)
ENGINE = ReplacingMergeTree
ORDER BY (ns, run_id, unit, at);

-- Messages that failed to parse, from any of the topic families.
CREATE TABLE IF NOT EXISTS tb.ingest_errors
(
    at DateTime64(3) DEFAULT now64(3),
    topic String,
    error String,
    raw String
)
ENGINE = MergeTree
ORDER BY at;

CREATE MATERIALIZED VIEW IF NOT EXISTS tb.mv_aggregates TO tb.aggregates AS
SELECT
    splitByChar('.', _topic)[1] AS ns,
    run_id,
    key,
    tenant,
    region,
    unit,
    fromUnixTimestamp64Milli(minute_ms, 'UTC') AS minute,
    count,
    errors,
    p95_latency_ms
FROM tb.kafka_aggregates
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS tb.mv_aggregates_errors TO tb.ingest_errors AS
SELECT
    _topic AS topic,
    _error AS error,
    _raw_message AS raw
FROM tb.kafka_aggregates
WHERE length(_error) > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS tb.mv_scores TO tb.scores AS
SELECT
    splitByChar('.', _topic)[1] AS ns,
    run_id,
    key,
    tenant,
    region,
    unit,
    signal,
    fromUnixTimestamp64Milli(minute_ms, 'UTC') AS minute,
    value,
    median,
    spread,
    score,
    source
FROM tb.kafka_scores
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS tb.mv_scores_errors TO tb.ingest_errors AS
SELECT
    _topic AS topic,
    _error AS error,
    _raw_message AS raw
FROM tb.kafka_scores
WHERE length(_error) > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS tb.mv_alerts TO tb.alert_log AS
SELECT
    splitByChar('.', _topic)[1] AS ns,
    alert_id,
    run_id,
    detector,
    setting,
    scope,
    key,
    tenant,
    region,
    unit,
    signal,
    state,
    fromUnixTimestamp64Milli(starts_at_ms, 'UTC') AS starts_at,
    fromUnixTimestamp64Milli(event_time_ms, 'UTC') AS event_time,
    fromUnixTimestamp64Milli(emitted_at_ms, 'UTC') AS emitted_at,
    value,
    reference,
    reason
FROM tb.kafka_alerts
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS tb.mv_alerts_errors TO tb.ingest_errors AS
SELECT
    _topic AS topic,
    _error AS error,
    _raw_message AS raw
FROM tb.kafka_alerts
WHERE length(_error) > 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS tb.mv_unit_status TO tb.unit_status AS
SELECT
    splitByChar('.', _topic)[1] AS ns,
    run_id,
    unit,
    region,
    status,
    signal,
    fromUnixTimestamp64Milli(at_ms, 'UTC') AS at,
    fromUnixTimestamp64Milli(emitted_at_ms, 'UTC') AS emitted_at,
    reason
FROM tb.kafka_unit_status
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS tb.mv_unit_status_errors TO tb.ingest_errors AS
SELECT
    _topic AS topic,
    _error AS error,
    _raw_message AS raw
FROM tb.kafka_unit_status
WHERE length(_error) > 0;
