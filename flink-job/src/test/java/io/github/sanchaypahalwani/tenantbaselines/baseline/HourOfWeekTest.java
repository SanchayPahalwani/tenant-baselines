package io.github.sanchaypahalwani.tenantbaselines.baseline;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HourOfWeekTest {

  @Test
  void mondayMidnightIsBucketZero() {
    assertThat(HourOfWeek.bucket(345_600_000L)).isEqualTo(0);
    assertThat(HourOfWeek.week(345_600_000L)).isEqualTo(1);
    assertThat(HourOfWeek.bucket(345_599_999L)).isEqualTo(167);
    assertThat(HourOfWeek.week(345_599_999L)).isEqualTo(0);
  }

  @Test
  void negativeTimesBucketLikePositiveOnes() {
    assertThat(HourOfWeek.hourStart(-1)).isEqualTo(-3_600_000L);
    assertThat(HourOfWeek.bucket(-1)).isEqualTo(71);
    assertThat(HourOfWeek.bucket(604_799_999L)).isEqualTo(71);
  }

  @Test
  void epochIsThursday() {
    assertThat(HourOfWeek.bucket(0)).isEqualTo(72);
    assertThat(HourOfWeek.minuteOfHour(3_660_000L)).isEqualTo(1);
    assertThat(HourOfWeek.hourStart(3_660_000L)).isEqualTo(3_600_000L);
  }
}
