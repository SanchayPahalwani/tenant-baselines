package io.github.sanchaypahalwani.tenantbaselines.baseline;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.Test;

class BucketHistoryTest {
  private static final Signal S = Signal.LATENCY_P95;

  private static void fill(BucketHistory h, long week, int count, double value) {
    for (int m = 0; m < count; m++) {
      h.put(week, m, S, value);
    }
  }

  private static BucketHistory twoWeeks() {
    BucketHistory h = new BucketHistory();
    for (long week = 10; week <= 11; week++) {
      for (int m = 0; m < 60; m++) {
        h.put(week, m, S, 100 + (m % 3));
      }
    }
    return h;
  }

  @Test
  void baselineIsMedianAndScaledMadOfPastWeeks() {
    BucketHistory h = twoWeeks();
    Baseline b = h.baseline(S, 12);
    assertThat(b.median()).isEqualTo(101.0);
    assertThat(b.scaledMad()).isEqualTo(1.4826);
    assertThat(h.weeksWithData(S, 12)).isEqualTo(2);
  }

  @Test
  void currentWeekIsExcluded() {
    BucketHistory h = twoWeeks();
    fill(h, 12, 60, 900);
    assertThat(h.baseline(S, 12).median()).isEqualTo(101.0);
  }

  @Test
  void onlyLastFourWeeksCount() {
    BucketHistory h = new BucketHistory();
    for (long week = 10; week <= 14; week++) {
      fill(h, week, 60, week - 9);
    }
    assertThat(h.baseline(S, 15).median()).isEqualTo(3.5);
    assertThat(h.weeksWithData(S, 15)).isEqualTo(4);
  }

  @Test
  void weekNeedsThirtyMinutes() {
    BucketHistory h = new BucketHistory();
    fill(h, 10, 29, 5);
    fill(h, 11, 60, 5);
    assertThat(h.weeksWithData(S, 12)).isEqualTo(1);

    BucketHistory twentyNine = new BucketHistory();
    fill(twentyNine, 10, 29, 5);
    assertThat(twentyNine.weeksWithData(S, 12)).isEqualTo(0);
    BucketHistory thirty = new BucketHistory();
    fill(thirty, 10, 30, 5);
    assertThat(thirty.weeksWithData(S, 12)).isEqualTo(1);
  }

  @Test
  void olderWeekDoesNotWipeANewerOne() {
    BucketHistory h = new BucketHistory();
    h.put(9, 0, Signal.ERROR_RATE, 0.5);
    h.put(4, 0, Signal.ERROR_RATE, 0.9); // week 4 shares slot 4 with week 9
    assertThat(h.baseline(Signal.ERROR_RATE, 10).median()).isEqualTo(0.5);
  }

  @Test
  void signalsAreWipedTogether() {
    BucketHistory h = new BucketHistory();
    for (Signal s : Signal.values()) {
      h.put(4, 0, s, 1.0);
    }
    h.put(9, 1, Signal.ERROR_RATE, 0.5);
    assertThat(h.baseline(Signal.LATENCY_P95, 10)).isNull();
    assertThat(h.baseline(Signal.VOLUME_5M, 10)).isNull();
  }

  @Test
  void slotIsWipedWhenReused() {
    BucketHistory h = new BucketHistory();
    fill(h, 10, 60, 7);
    h.put(15, 0, S, 42);
    assertThat(h.baseline(S, 16).median()).isEqualTo(42.0);
    assertThat(h.weeksWithData(S, 16)).isEqualTo(0);
  }

  @Test
  void staleSlotsAreIgnored() {
    BucketHistory h = twoWeeks();
    assertThat(h.baseline(S, 30)).isNull();
    assertThat(h.weeksWithData(S, 30)).isEqualTo(0);
  }

  @Test
  void signalsAreIndependent() {
    BucketHistory h = new BucketHistory();
    for (long week = 10; week <= 11; week++) {
      for (int m = 0; m < 60; m++) {
        h.put(week, m, Signal.ERROR_RATE, 0.01);
      }
    }
    assertThat(h.baseline(Signal.VOLUME_5M, 12)).isNull();
    assertThat(h.baseline(Signal.ERROR_RATE, 12)).isNotNull();
  }

  @Test
  void stateClassesAreFlinkPojos() {
    assertThat(TypeInformation.of(BucketHistory.class)).isInstanceOf(PojoTypeInfo.class);
    assertThat(TypeInformation.of(HourCache.class)).isInstanceOf(PojoTypeInfo.class);
  }
}
