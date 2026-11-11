package io.github.sanchaypahalwani.tenantbaselines.detect;

import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;

/** Builds every {@link AlertEvent} of the job, so all operators write the same shape. */
public final class Alerts {

  public static final String FIRING = "firing";
  public static final String RESOLVED = "resolved";
  public static final String ABSORBED = "absorbed";
  public static final String TENANT = "tenant";
  public static final String REGION = "region";

  private Alerts() {}

  /**
   * The alert id is {@link AlertIds#of} over detector, setting, scope, key, signal and start. A
   * value or reference that is NaN or infinite (an error-rate reference on a minute without
   * requests, say) is written as 0.0.
   */
  public static AlertEvent of(
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
      double value,
      double reference,
      String reason) {
    return new AlertEvent(
        AlertIds.of(detector, setting, scope, key, signal, startsAtMs),
        runId,
        detector,
        setting,
        scope,
        key,
        tenant,
        region,
        unit,
        signal,
        state,
        startsAtMs,
        eventTimeMs,
        System.currentTimeMillis(),
        finiteOrZero(value),
        finiteOrZero(reference),
        reason);
  }

  public static double finiteOrZero(double v) {
    return Double.isFinite(v) ? v : 0.0;
  }
}
