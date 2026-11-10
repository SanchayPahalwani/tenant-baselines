package io.github.sanchaypahalwani.tenantbaselines.baseline;

import io.github.sanchaypahalwani.tenantbaselines.agg.LatencyHistogram;

/**
 * Counts of scored minutes by error-rate bin and by latency bucket, for one scope, hour-of-week
 * bucket and week. Each minute adds one count to each array. Flink state, so it stays a POJO.
 */
public class PopHistogram {
  public int[] errorRate = new int[ErrorRateBins.BINS];
  public int[] latency = new int[LatencyHistogram.BUCKETS];

  public void add(double rate, double p95Ms) {
    errorRate[ErrorRateBins.bucketOf(rate)]++;
    latency[LatencyHistogram.bucketOf(p95Ms)]++;
  }

  public void addAll(PopHistogram other) {
    for (int i = 0; i < errorRate.length; i++) {
      errorRate[i] += other.errorRate[i];
    }
    for (int i = 0; i < latency.length; i++) {
      latency[i] += other.latency[i];
    }
  }

  /** The number of minutes counted. */
  public long n() {
    long n = 0;
    for (int c : latency) {
      n += c;
    }
    return n;
  }
}
