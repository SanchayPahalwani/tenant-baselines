package io.github.sanchaypahalwani.tenantbaselines.baseline;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import org.junit.jupiter.api.Test;

class HourCacheTest {
  @Test
  void warmNeedsTwoWeeks() {
    BucketHistory h = new BucketHistory();
    for (long week = 10; week <= 11; week++) {
      for (int m = 0; m < 60; m++) {
        for (Signal s : Signal.values()) {
          h.put(week, m, s, 5);
        }
      }
    }
    long hour = 12 * HourOfWeek.WEEK_MS - 3 * 24 * HourOfWeek.HOUR_MS; // Monday 00:00 of week 12
    assertThat(HourOfWeek.week(hour)).isEqualTo(12);
    HourCache c = HourCache.of(h, hour);
    for (Signal s : Signal.values()) {
      assertThat(c.warm[s.ordinal()]).isTrue();
      assertThat(c.own(s)).isNotNull();
    }
    assertThat(c.own(Signal.VOLUME_5M).median()).isEqualTo(5.0);

    HourCache oneWeek = HourCache.of(h, 11 * HourOfWeek.WEEK_MS - 3 * 24 * HourOfWeek.HOUR_MS);
    assertThat(oneWeek.own(Signal.ERROR_RATE)).isNull();
  }

  @Test
  void nullHistoryIsCold() {
    HourCache c = HourCache.of(null, 3_600_000L);
    assertThat(c.hourStartMs).isEqualTo(3_600_000L);
    for (Signal s : Signal.values()) {
      assertThat(c.own(s)).isNull();
    }
  }
}
