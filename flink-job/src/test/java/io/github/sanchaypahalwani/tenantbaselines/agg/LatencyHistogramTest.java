package io.github.sanchaypahalwani.tenantbaselines.agg;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LatencyHistogramTest {

  @Test
  void tableIsAscendingAndComplete() {
    double[] b = LatencyHistogram.bounds();
    assertThat(b).hasSize(202);
    assertThat(b[0]).isEqualTo(0.5);
    for (int i = 1; i < b.length; i++) {
      assertThat(b[i]).isGreaterThan(b[i - 1]);
    }
  }

  @Test
  void bucketsClampAtBothEnds() {
    assertThat(LatencyHistogram.bucketOf(0.01)).isEqualTo(0);
    assertThat(LatencyHistogram.bucketOf(1e9)).isEqualTo(201);
  }

  @Test
  void representativeWithinThreePercent() {
    double[] b = LatencyHistogram.bounds();
    for (int i = 0; i < 201; i++) {
      double rep = LatencyHistogram.representative(i);
      for (double x : new double[] {b[i], b[i + 1] - 1e-6}) {
        assertThat(Math.abs(rep - x) / x).as("bucket %d x=%s", i, x).isLessThanOrEqualTo(0.03);
      }
    }
  }

  @Test
  void p95OfOneToHundred() {
    int[] counts = new int[LatencyHistogram.BUCKETS];
    for (int ms = 1; ms <= 100; ms++) {
      counts[LatencyHistogram.bucketOf(ms)]++;
    }
    assertThat(LatencyHistogram.p95(counts, 100))
        .isEqualTo(LatencyHistogram.representative(LatencyHistogram.bucketOf(95.0)));
  }

  @Test
  void p95OfEmptyIsNull() {
    assertThat(LatencyHistogram.p95(new int[LatencyHistogram.BUCKETS], 0)).isNull();
  }
}
