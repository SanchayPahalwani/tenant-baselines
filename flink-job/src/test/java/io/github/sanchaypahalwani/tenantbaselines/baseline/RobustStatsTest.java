package io.github.sanchaypahalwani.tenantbaselines.baseline;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RobustStatsTest {

  @Test
  void medianOddAndEven() {
    assertThat(RobustStats.median(new double[] {5, 1, 3}, 3)).isEqualTo(3.0);
    assertThat(RobustStats.median(new double[] {4, 1, 3, 2}, 4)).isEqualTo(2.5);
    assertThat(RobustStats.median(new double[] {}, 0)).isNaN();
  }

  @Test
  void madIgnoresOneOutlier() {
    double[] v = {1, 2, 3, 4, 100};
    double median = RobustStats.median(v, 5);
    assertThat(median).isEqualTo(3.0);
    assertThat(RobustStats.mad(v, 5, median)).isEqualTo(1.0);
  }
}
