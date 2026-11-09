package io.github.sanchaypahalwani.tenantbaselines.model;

/** Per-key, per-minute aggregate. {@code p95LatencyMs} is null when {@code count == 0}. */
public record MinuteAgg(
    String runId,
    String key,
    String tenant,
    String region,
    String unit,
    long minuteMs,
    long count,
    long errors,
    Double p95LatencyMs) {}
