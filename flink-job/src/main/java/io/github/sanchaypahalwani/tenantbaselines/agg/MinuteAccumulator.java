package io.github.sanchaypahalwani.tenantbaselines.agg;

/** Mutable per-minute state: request count, error count and latency bucket counts. */
public class MinuteAccumulator {

  public long count;
  public long errors;
  public int[] buckets = new int[LatencyHistogram.BUCKETS];

  public MinuteAccumulator() {}
}
