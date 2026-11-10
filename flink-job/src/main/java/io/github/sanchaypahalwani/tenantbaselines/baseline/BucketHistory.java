package io.github.sanchaypahalwani.tenantbaselines.baseline;

import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import java.util.Arrays;

/**
 * One-minute values for one hour-of-week bucket of one key, kept in a ring of weekly slots. A slot
 * holds one week; the slot of a week that is 5 weeks old is reused. This is Flink state, so it
 * stays a POJO with public non-final fields.
 */
public class BucketHistory {
  public static final int SLOTS = 5;
  public static final int MINUTES = 60;
  public static final int BASELINE_WEEKS = 4;
  public static final int MIN_MINUTES_PER_WEEK = 30;
  public static final int MIN_WEEKS = 2;

  private static final int SIGNALS = Signal.values().length;

  /** The week held by each slot, or Long.MIN_VALUE when the slot is empty. */
  public long[] slotWeek = new long[SLOTS];

  /** NaN means no value. Index: (signal * SLOTS + slot) * MINUTES + minuteOfHour. */
  public float[] values = new float[SIGNALS * SLOTS * MINUTES];

  public BucketHistory() {
    Arrays.fill(slotWeek, Long.MIN_VALUE);
    Arrays.fill(values, Float.NaN);
  }

  private static int index(Signal s, int slot, int minuteOfHour) {
    return (s.ordinal() * SLOTS + slot) * MINUTES + minuteOfHour;
  }

  public void put(long week, int minuteOfHour, Signal s, double value) {
    int slot = (int) Math.floorMod(week, (long) SLOTS);
    if (slotWeek[slot] != week) {
      for (Signal each : Signal.values()) {
        int from = index(each, slot, 0);
        Arrays.fill(values, from, from + MINUTES, Float.NaN);
      }
      slotWeek[slot] = week;
    }
    values[index(s, slot, minuteOfHour)] = (float) value;
  }

  private boolean inWindow(int slot, long currentWeek) {
    long w = slotWeek[slot];
    return w != Long.MIN_VALUE && w >= currentWeek - BASELINE_WEEKS && w < currentWeek;
  }

  private int countValues(Signal s, int slot) {
    int from = index(s, slot, 0);
    int n = 0;
    for (int i = from; i < from + MINUTES; i++) {
      if (!Float.isNaN(values[i])) {
        n++;
      }
    }
    return n;
  }

  /** Weeks in the baseline window with at least MIN_MINUTES_PER_WEEK values for the signal. */
  public int weeksWithData(Signal s, long currentWeek) {
    int weeks = 0;
    for (int slot = 0; slot < SLOTS; slot++) {
      if (inWindow(slot, currentWeek) && countValues(s, slot) >= MIN_MINUTES_PER_WEEK) {
        weeks++;
      }
    }
    return weeks;
  }

  /** Median and scaled MAD of the window's values, or null when the window has none. */
  public Baseline baseline(Signal s, long currentWeek) {
    double[] scratch = new double[BASELINE_WEEKS * MINUTES];
    int n = 0;
    for (int slot = 0; slot < SLOTS; slot++) {
      if (!inWindow(slot, currentWeek)) {
        continue;
      }
      int from = index(s, slot, 0);
      for (int i = from; i < from + MINUTES; i++) {
        if (!Float.isNaN(values[i])) {
          scratch[n++] = values[i];
        }
      }
    }
    if (n == 0) {
      return null;
    }
    double median = RobustStats.median(scratch, n);
    double mad = RobustStats.mad(scratch, n, median);
    return new Baseline(median, RobustStats.MAD_SCALE * mad);
  }
}
