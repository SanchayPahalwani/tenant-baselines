package io.github.sanchaypahalwani.tenantbaselines.group;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KeyActivityTest {

  private static long min(long n) {
    return n * 60_000L;
  }

  @Test
  void activeAtTwelveHundredInTheHour() {
    var k = new KeyActivity();
    for (int m = 0; m < 60; m++) {
      k.add(min(m), 20);
    }
    assertThat(k.sum).isEqualTo(1200);
    assertThat(k.active).isTrue();

    var j = new KeyActivity();
    for (int m = 0; m < 59; m++) {
      j.add(min(m), 20);
    }
    j.add(min(59), 19);
    assertThat(j.sum).isEqualTo(1199);
    assertThat(j.active).isFalse();
  }

  @Test
  void oldMinutesLeaveTheHour() {
    var k = new KeyActivity();
    k.add(min(0), 1200);
    for (int m = 1; m < 60; m++) {
      k.add(min(m), 0);
    }
    assertThat(k.active).isTrue();

    k.add(min(60), 0);
    assertThat(k.active).isFalse();
    assertThat(k.sum).isZero();
  }

  @Test
  void gapClearsTheRing() {
    var k = new KeyActivity();
    k.add(min(0), 1200);
    k.add(min(200), 5);
    assertThat(k.sum).isEqualTo(5);
    assertThat(k.active).isFalse();
  }

  @Test
  void skippedMinutesCountZero() {
    var k = new KeyActivity();
    k.add(min(0), 1200);
    k.add(min(30), 7);
    // Minutes 31 to 60 are skipped; minute 0 leaves the hour on the way.
    k.add(min(61), 5);
    assertThat(k.sum).isEqualTo(12);
    assertThat(k.active).isFalse();
  }

  @Test
  void olderMinuteIsIgnored() {
    var k = new KeyActivity();
    k.add(min(5), 100);
    k.add(min(3), 999);
    assertThat(k.sum).isEqualTo(100);
    assertThat(k.lastMinuteMs).isEqualTo(min(5));
  }
}
