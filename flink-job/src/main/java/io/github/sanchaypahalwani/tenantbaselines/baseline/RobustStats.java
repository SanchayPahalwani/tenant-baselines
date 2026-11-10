package io.github.sanchaypahalwani.tenantbaselines.baseline;

import java.util.Arrays;

/** Median and median absolute deviation. */
public final class RobustStats {
  /** Makes the MAD comparable to a standard deviation for normal data. */
  public static final double MAD_SCALE = 1.4826;

  private RobustStats() {}

  /** Sorts the first n entries in place. Returns NaN when n is 0. */
  public static double median(double[] v, int n) {
    if (n == 0) {
      return Double.NaN;
    }
    Arrays.sort(v, 0, n);
    return n % 2 == 1 ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2.0;
  }

  /** The median of |v[i] - median| over the first n entries. Reorders v. */
  public static double mad(double[] v, int n, double median) {
    for (int i = 0; i < n; i++) {
      v[i] = Math.abs(v[i] - median);
    }
    return median(v, n);
  }
}
