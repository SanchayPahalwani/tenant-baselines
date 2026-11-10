package io.github.sanchaypahalwani.tenantbaselines.baseline;

import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import java.io.Serializable;

/**
 * The fallback baseline of one scope (a region, or {@link #GLOBAL}) for one hour-of-week bucket,
 * to be used in {@code week}. Scaled MADs are already 1.4826 times the MAD; {@code n} is the
 * number of minutes behind it.
 */
public record PopulationBaseline(
    String scope,
    int bucket,
    long week,
    double errorMedian,
    double errorScaledMad,
    double latencyMedian,
    double latencyScaledMad,
    long n)
    implements Serializable {

  public static final String GLOBAL = "*";

  /** The baseline for the signal; null for volume, which has no fallback. */
  public Baseline of(Signal s) {
    return switch (s) {
      case ERROR_RATE -> new Baseline(errorMedian, errorScaledMad);
      case LATENCY_P95 -> new Baseline(latencyMedian, latencyScaledMad);
      case VOLUME_5M -> null;
    };
  }
}
