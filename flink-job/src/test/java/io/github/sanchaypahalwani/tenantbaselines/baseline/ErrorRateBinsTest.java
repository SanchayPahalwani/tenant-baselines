package io.github.sanchaypahalwani.tenantbaselines.baseline;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ErrorRateBinsTest {

  @Test
  void zeroHasItsOwnBin() {
    assertThat(ErrorRateBins.bucketOf(0)).isZero();
    assertThat(ErrorRateBins.representative(0)).isZero();
    assertThat(ErrorRateBins.bucketOf(1e-4)).isEqualTo(1);
    assertThat(ErrorRateBins.bucketOf(1.0)).isEqualTo(159);
  }

  @Test
  void representativeWithinThreePercent() {
    for (double rate : new double[] {0.001, 0.01, 0.2, 0.9}) {
      double rep = ErrorRateBins.representative(ErrorRateBins.bucketOf(rate));
      assertThat(Math.abs(rep - rate) / rate).as("rate %s", rate).isLessThanOrEqualTo(0.03);
    }
  }
}
