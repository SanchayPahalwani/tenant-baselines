package io.github.sanchaypahalwani.tenantbaselines.detect;

/**
 * The counts of the last five consecutive minutes. A gap in minutes empties it. This is a Flink
 * POJO: keep it public with public non-final fields.
 */
public class VolumeRing {

  public long lastMinuteMs = Long.MIN_VALUE;
  public long[] counts = new long[5];
  public int filled;

  public VolumeRing() {}

  /** Returns the 5-minute sum, or NaN until five consecutive minutes have been seen. */
  public double push(long minuteMs, long count) {
    if (lastMinuteMs == Long.MIN_VALUE || minuteMs != lastMinuteMs + 60_000L) {
      filled = 0;
    }
    lastMinuteMs = minuteMs;
    System.arraycopy(counts, 1, counts, 0, counts.length - 1);
    counts[counts.length - 1] = count;
    if (filled < counts.length) {
      filled++;
    }
    if (filled < counts.length) {
      return Double.NaN;
    }
    long sum = 0;
    for (long c : counts) {
      sum += c;
    }
    return sum;
  }
}
