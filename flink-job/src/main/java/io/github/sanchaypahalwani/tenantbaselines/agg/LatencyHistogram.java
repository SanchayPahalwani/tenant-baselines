package io.github.sanchaypahalwani.tenantbaselines.agg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Log-spaced latency buckets. The lower bounds come from the shared file
 * {@code latency_bounds_ms.txt}, so Java and Python bucket identically.
 */
public final class LatencyHistogram {

  public static final int BUCKETS = 202;

  private static final double[] BOUNDS = load();

  private LatencyHistogram() {}

  private static double[] load() {
    try (InputStream in = LatencyHistogram.class.getResourceAsStream("/latency_bounds_ms.txt")) {
      if (in == null) {
        throw new IllegalStateException("latency_bounds_ms.txt not found on classpath");
      }
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      List<Double> values = new ArrayList<>();
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty()) {
          values.add(Double.parseDouble(line));
        }
      }
      if (values.size() != BUCKETS) {
        throw new IllegalStateException(
            "expected " + BUCKETS + " bounds but found " + values.size());
      }
      double[] out = new double[BUCKETS];
      for (int i = 0; i < BUCKETS; i++) {
        out[i] = values.get(i);
      }
      return out;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Lower bounds of each bucket in milliseconds. Callers must not modify the array. */
  public static double[] bounds() {
    return BOUNDS;
  }

  /** Index of the largest bound that is at most the latency; 0 below the first bound. */
  public static int bucketOf(double latencyMs) {
    int lo = 0;
    int hi = BUCKETS - 1;
    while (lo < hi) {
      int mid = (lo + hi + 1) >>> 1;
      if (BOUNDS[mid] <= latencyMs) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }

  /** Geometric midpoint of the bucket; the last bucket is open-ended and uses its lower bound. */
  public static double representative(int bucket) {
    if (bucket >= BUCKETS - 1) {
      return BOUNDS[BUCKETS - 1];
    }
    return Math.sqrt(BOUNDS[bucket] * BOUNDS[bucket + 1]);
  }

  /** 95th percentile estimate from bucket counts, or null when there are no samples. */
  public static Double p95(int[] counts, long n) {
    if (n == 0) {
      return null;
    }
    long rank = (long) Math.ceil(0.95 * n);
    long cumulative = 0;
    for (int i = 0; i < counts.length; i++) {
      cumulative += counts[i];
      if (cumulative >= rank) {
        return representative(i);
      }
    }
    return representative(counts.length - 1);
  }
}
