package io.github.sanchaypahalwani.tenantbaselines.model;

/**
 * One signal of one key and minute with the baseline it was judged against. {@code median} is the
 * value the score is measured from (for volume, the expected 5-minute sum) and {@code source} says
 * whose baseline it is: "own", "region" or "global", or null when there is none. A number that
 * does not exist is null, never NaN or infinite.
 */
public record ScoreEvent(
    String runId,
    String key,
    String tenant,
    String region,
    String unit,
    String signal,
    long minuteMs,
    Double value,
    Double median,
    Double spread,
    Double score,
    String source) {}
