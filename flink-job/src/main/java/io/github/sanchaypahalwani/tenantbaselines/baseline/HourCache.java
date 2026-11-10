package io.github.sanchaypahalwani.tenantbaselines.baseline;

import io.github.sanchaypahalwani.tenantbaselines.model.Signal;

/**
 * The baselines of one bucket for one hour, computed once because the current week is not part of
 * its own baseline. Indexed by Signal.ordinal(). Flink state, so it stays a POJO.
 */
public class HourCache {
  public long hourStartMs = Long.MIN_VALUE;
  public double[] median = new double[3];
  public double[] scaledMad = new double[3];
  public boolean[] warm = new boolean[3];

  /** h may be null, which leaves every signal cold. */
  public static HourCache of(BucketHistory h, long hourStartMs) {
    HourCache c = new HourCache();
    c.hourStartMs = hourStartMs;
    if (h == null) {
      return c;
    }
    long week = HourOfWeek.week(hourStartMs);
    for (Signal s : Signal.values()) {
      int i = s.ordinal();
      if (h.weeksWithData(s, week) >= BucketHistory.MIN_WEEKS) {
        Baseline b = h.baseline(s, week);
        if (b != null) {
          c.median[i] = b.median();
          c.scaledMad[i] = b.scaledMad();
          c.warm[i] = true;
        }
      }
    }
    return c;
  }

  /** The baseline when warm for the signal, else null. */
  public Baseline own(Signal s) {
    int i = s.ordinal();
    return warm[i] ? new Baseline(median[i], scaledMad[i]) : null;
  }
}
