package io.github.sanchaypahalwani.tenantbaselines.baseline;

/** Hour-of-week buckets in UTC. Weeks start on Monday 00:00. */
public final class HourOfWeek {
  public static final long HOUR_MS = 3_600_000L;
  public static final long WEEK_MS = 604_800_000L;

  /**
   * 1970-01-01 was a Thursday, so the first Monday, 1970-01-05, is four days after the epoch.
   * Adding 3 days (259,200,000 ms) maps every Monday 00:00 UTC onto a multiple of 7 days.
   */
  private static final long MONDAY_SHIFT_MS = 259_200_000L;

  private HourOfWeek() {}

  /** The week number, counting from the first Monday after the epoch. */
  public static long week(long ms) {
    return Math.floorDiv(ms + MONDAY_SHIFT_MS, WEEK_MS);
  }

  /** The hour of the week, 0 (Monday 00:00) to 167 (Sunday 23:00). */
  public static int bucket(long ms) {
    return (int) (Math.floorMod(ms + MONDAY_SHIFT_MS, WEEK_MS) / HOUR_MS);
  }

  public static int minuteOfHour(long ms) {
    return (int) (Math.floorMod(ms, HOUR_MS) / 60_000L);
  }

  public static long hourStart(long ms) {
    return ms - Math.floorMod(ms, HOUR_MS);
  }
}
