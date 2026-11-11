package io.github.sanchaypahalwani.tenantbaselines.group;

import java.util.Arrays;

/**
 * The request counts of one key over its last {@link #MINUTES} minutes, in a ring indexed by
 * minute. The key is active while they add up to at least {@link #ACTIVE_REQUESTS}. Expects its
 * minutes in order; a minute older than the newest one changes nothing. This is a Flink POJO: keep
 * it public with public non-final fields.
 */
public class KeyActivity {

  public static final int MINUTES = 60;
  public static final int ACTIVE_REQUESTS = 1200;

  private static final long MINUTE_MS = 60_000L;

  public String tenant;
  public long lastMinuteMs = Long.MIN_VALUE;
  public int[] counts = new int[MINUTES];
  public long sum;
  public boolean active;

  public KeyActivity() {}

  /**
   * Moves the ring on to minuteMs and stores its count. Skipped minutes count 0; a gap of {@link
   * #MINUTES} minutes or more clears the ring.
   */
  public void add(long minuteMs, long count) {
    if (lastMinuteMs != Long.MIN_VALUE && minuteMs < lastMinuteMs) {
      return;
    }
    if (lastMinuteMs == Long.MIN_VALUE || minuteMs - lastMinuteMs >= MINUTES * MINUTE_MS) {
      Arrays.fill(counts, 0);
      sum = 0;
    } else {
      for (long skipped = lastMinuteMs + MINUTE_MS; skipped < minuteMs; skipped += MINUTE_MS) {
        int i = slot(skipped);
        sum -= counts[i];
        counts[i] = 0;
      }
    }
    int i = slot(minuteMs);
    sum -= counts[i];
    counts[i] = (int) Math.min(count, Integer.MAX_VALUE);
    sum += counts[i];
    lastMinuteMs = minuteMs;
    active = sum >= ACTIVE_REQUESTS;
  }

  private static int slot(long minuteMs) {
    return (int) Math.floorMod(Math.floorDiv(minuteMs, MINUTE_MS), (long) MINUTES);
  }
}
