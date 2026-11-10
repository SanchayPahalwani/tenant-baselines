package io.github.sanchaypahalwani.tenantbaselines.baseline;

/**
 * Log-spaced error-rate bins, 6% wide, from 1e-4 up. Bin 0 holds rates below 1e-4, including 0;
 * the last bin also holds every rate past its lower bound.
 */
public final class ErrorRateBins {
  public static final int BINS = 160;

  private static final double LOWEST = 1e-4;
  private static final double GROWTH = 1.06;
  private static final double LOG_GROWTH = Math.log(GROWTH);
  private static final double HALF_STEP = Math.sqrt(GROWTH);

  private ErrorRateBins() {}

  public static int bucketOf(double rate) {
    if (rate < LOWEST) {
      return 0;
    }
    return 1 + Math.min(BINS - 2, (int) Math.floor(Math.log(rate / LOWEST) / LOG_GROWTH));
  }

  /** 0 for bin 0, else the bin's geometric midpoint, capped at 1. */
  public static double representative(int bin) {
    if (bin == 0) {
      return 0;
    }
    return Math.min(1.0, LOWEST * Math.pow(GROWTH, bin - 1) * HALF_STEP);
  }
}
