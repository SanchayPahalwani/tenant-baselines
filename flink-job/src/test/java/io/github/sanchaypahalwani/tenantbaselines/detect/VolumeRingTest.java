package io.github.sanchaypahalwani.tenantbaselines.detect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.Test;

class VolumeRingTest {

  private static VolumeRing fiveWith(double[] medians, double[] scaledMads) {
    var r = new VolumeRing();
    for (int i = 0; i < 5; i++) {
      r.push(i * 60_000L, 1, medians[i], scaledMads[i]);
    }
    return r;
  }

  @Test
  void volumeRingSumsFiveConsecutiveMinutesAndResetsOnGap() {
    var r = new VolumeRing();
    assertThat(r.push(0, 1)).isNaN();
    assertThat(r.push(60_000, 2)).isNaN();
    assertThat(r.push(120_000, 3)).isNaN();
    assertThat(r.push(180_000, 4)).isNaN();
    assertThat(r.push(240_000, 5)).isEqualTo(15);
    assertThat(r.push(300_000, 6)).isEqualTo(20);
    assertThat(r.push(420_000, 7)).isNaN();
    assertThat(r.push(480_000, 1)).isNaN();
  }

  @Test
  void expectedIsSumOfFiveMedians() {
    var r = fiveWith(new double[] {10, 20, 30, 40, 50}, new double[] {3, 3, 3, 3, 3});
    assertThat(r.expected()).isEqualTo(150);
    assertThat(r.spread()).isCloseTo(Math.max(Math.sqrt(45), Math.sqrt(150)), within(1e-9));
    assertThat(r.spread()).isCloseTo(12.247, within(1e-3));
  }

  @Test
  void spreadUsesEachMinutesMad() {
    // Median 100 each; the last two minutes (after an hour boundary) carry their own MAD.
    var r = fiveWith(new double[] {100, 100, 100, 100, 100}, new double[] {1, 1, 1, 30, 30});
    assertThat(r.spread()).isCloseTo(Math.sqrt(1803), within(1e-9));
  }

  @Test
  void madTermWinsWhenLarger() {
    var r = fiveWith(new double[] {4, 4, 4, 4, 4}, new double[] {5, 5, 5, 5, 5});
    assertThat(r.spread()).isCloseTo(Math.sqrt(5) * 5, within(1e-9));
    assertThat(r.spread()).isCloseTo(11.180, within(1e-3));
  }

  @Test
  void expectedIsNaNWhileAnyMinuteIsCold() {
    var r = fiveWith(new double[] {10, 20, Double.NaN, 40, 50}, new double[] {3, 3, 3, 3, 3});
    assertThat(r.expected()).isNaN();
    assertThat(r.spread()).isNaN();
  }

  @Test
  void expectedIsNaNUntilFiveMinutesAreHeld() {
    var r = new VolumeRing();
    r.push(0, 1, 10, 1);
    assertThat(r.expected()).isNaN();
    assertThat(r.spread()).isNaN();
  }

  @Test
  void trailingExpectedUsesThirtyMinutesBeforeTheWindow() {
    var r = new VolumeRing();
    long t = 0;
    for (int i = 0; i < 30; i++, t += 60_000) {
      r.push(t, 4);
    }
    for (int i = 0; i < 4; i++, t += 60_000) {
      r.push(t, 0);
    }
    assertThat(r.trailingExpected()).isNaN();
    r.push(t, 0);
    assertThat(r.trailingExpected()).isEqualTo(20);
  }

  @Test
  void gapResetsEverything() {
    var r = new VolumeRing();
    long t = 0;
    for (int i = 0; i < 35; i++, t += 60_000) {
      r.push(t, 4, 4, 1);
    }
    assertThat(r.expected()).isEqualTo(20);
    assertThat(r.trailingExpected()).isEqualTo(20);
    r.push(t + 60_000, 4, 4, 1);
    assertThat(r.expected()).isNaN();
    assertThat(r.trailingExpected()).isNaN();
  }

  @Test
  void toleratesMissingNewFields() {
    var r = new VolumeRing();
    for (int i = 0; i < 4; i++) {
      r.push(i * 60_000L, i + 1);
    }
    r.medians = null;
    r.scaledMads = null;
    r.before = null;
    assertThat(r.push(240_000, 5)).isEqualTo(15);
    assertThat(r.expected()).isNaN();
    assertThat(r.trailingExpected()).isNaN();
    assertThat(r.push(300_000, 6, 2, 1)).isEqualTo(20);
    assertThat(r.expected()).isNaN();
    assertThat(r.medians).isNotNull();
    assertThat(r.scaledMads).isNotNull();
    assertThat(r.before).isNotNull();
  }

  @Test
  void restoredFullRingHasNoExpectationUntilFiveNewMinutes() {
    var r = new VolumeRing();
    for (int i = 0; i < 5; i++) {
      r.push(i * 60_000L, 1);
    }
    r.medians = null;
    r.scaledMads = null;
    r.before = null;
    for (int i = 5; i < 9; i++) {
      r.push(i * 60_000L, 1, 10, 2);
      assertThat(r.expected()).isNaN();
      assertThat(r.spread()).isNaN();
    }
    r.push(9 * 60_000L, 1, 10, 2);
    assertThat(r.expected()).isEqualTo(50);
    assertThat(r.spread()).isCloseTo(Math.max(Math.sqrt(5) * 2, Math.sqrt(50)), within(1e-9));
  }
}
