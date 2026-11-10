package io.github.sanchaypahalwani.tenantbaselines.detect;

import io.github.sanchaypahalwani.tenantbaselines.baseline.Scores;

/**
 * The last five consecutive minutes: their counts, their bucket medians and scaled MADs, and the
 * thirty counts that left the window before them. A gap in minutes empties it. This is a Flink
 * POJO: keep it public with public non-final fields. State written before the median, MAD and
 * history arrays existed restores them as null, so {@link #push} allocates any that are missing.
 */
public class VolumeRing {

  private static final int WINDOW = 5;
  private static final int HISTORY = 30;

  public long lastMinuteMs = Long.MIN_VALUE;
  public long[] counts = new long[WINDOW];
  public int filled;
  public double[] medians = nans();
  public double[] scaledMads = nans();
  public long[] before = new long[HISTORY];
  public int beforeFilled;

  public VolumeRing() {}

  private static double[] nans() {
    double[] a = new double[WINDOW];
    java.util.Arrays.fill(a, Double.NaN);
    return a;
  }

  /** Returns the 5-minute sum, or NaN until five consecutive minutes have been seen. */
  public double push(long minuteMs, long count) {
    return push(minuteMs, count, Double.NaN, Double.NaN);
  }

  /**
   * As {@link #push(long, long)}, also keeping the minute's bucket median and scaled MAD (NaN when
   * the key is cold there).
   */
  public double push(long minuteMs, long count, double median, double scaledMad) {
    if (medians == null) {
      medians = nans();
    }
    if (scaledMads == null) {
      scaledMads = nans();
    }
    if (before == null) {
      before = new long[HISTORY];
    }
    if (lastMinuteMs == Long.MIN_VALUE || minuteMs != lastMinuteMs + 60_000L) {
      filled = 0;
      beforeFilled = 0;
    }
    lastMinuteMs = minuteMs;
    if (filled == WINDOW) {
      System.arraycopy(before, 1, before, 0, HISTORY - 1);
      before[HISTORY - 1] = counts[0];
      if (beforeFilled < HISTORY) {
        beforeFilled++;
      }
    }
    System.arraycopy(counts, 1, counts, 0, WINDOW - 1);
    counts[WINDOW - 1] = count;
    System.arraycopy(medians, 1, medians, 0, WINDOW - 1);
    medians[WINDOW - 1] = median;
    System.arraycopy(scaledMads, 1, scaledMads, 0, WINDOW - 1);
    scaledMads[WINDOW - 1] = scaledMad;
    if (filled < WINDOW) {
      filled++;
    }
    if (filled < WINDOW) {
      return Double.NaN;
    }
    long sum = 0;
    for (long c : counts) {
      sum += c;
    }
    return sum;
  }

  /** Sum of the five bucket medians; NaN until five minutes are held or if any is NaN. */
  public double expected() {
    if (filled < WINDOW) {
      return Double.NaN;
    }
    double sum = 0;
    for (double m : medians) {
      sum += m;
    }
    return sum;
  }

  /** Spread of the 5-minute sum, NaN when {@link #expected()} is. */
  public double spread() {
    double expected = expected();
    if (Double.isNaN(expected)) {
      return Double.NaN;
    }
    double squares = 0;
    for (double s : scaledMads) {
      squares += s * s;
    }
    return Scores.volumeSpread(Math.sqrt(squares), expected);
  }

  /**
   * Five times the mean count of the thirty minutes before the current window; NaN until thirty
   * are held.
   */
  public double trailingExpected() {
    if (beforeFilled < HISTORY) {
      return Double.NaN;
    }
    long sum = 0;
    for (long c : before) {
      sum += c;
    }
    return WINDOW * (double) sum / HISTORY;
  }
}
