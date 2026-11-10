package io.github.sanchaypahalwani.tenantbaselines.baseline;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.agg.LatencyHistogram;
import org.junit.jupiter.api.Test;

class HistogramStatsTest {

  private static double rep(double latencyMs) {
    return LatencyHistogram.representative(LatencyHistogram.bucketOf(latencyMs));
  }

  @Test
  void medianAndMad() {
    int[] counts = new int[LatencyHistogram.BUCKETS];
    counts[LatencyHistogram.bucketOf(100)] += 40;
    counts[LatencyHistogram.bucketOf(90)] += 30;
    counts[LatencyHistogram.bucketOf(110)] += 30;

    double median = HistogramStats.median(counts, LatencyHistogram::representative);
    assertThat(median).isEqualTo(rep(100));
    double nearer = Math.min(Math.abs(rep(90) - median), Math.abs(rep(110) - median));
    assertThat(HistogramStats.mad(counts, LatencyHistogram::representative, median))
        .isEqualTo(nearer);

    int[] empty = new int[LatencyHistogram.BUCKETS];
    assertThat(HistogramStats.median(empty, LatencyHistogram::representative)).isNaN();
    assertThat(HistogramStats.mad(empty, LatencyHistogram::representative, Double.NaN)).isNaN();
  }
}
