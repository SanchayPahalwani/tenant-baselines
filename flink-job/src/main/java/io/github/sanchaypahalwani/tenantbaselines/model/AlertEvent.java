package io.github.sanchaypahalwani.tenantbaselines.model;

/**
 * A firing or resolved transition. {@code state} is "firing" or "resolved"; {@code eventTimeMs} is
 * the end of the minute that decided the transition; {@code reason} may be null.
 */
public record AlertEvent(
    String alertId,
    String runId,
    String detector,
    String setting,
    String scope,
    String key,
    String tenant,
    String region,
    String unit,
    String signal,
    String state,
    long startsAtMs,
    long eventTimeMs,
    long emittedAtMs,
    double value,
    double reference,
    String reason) {}
