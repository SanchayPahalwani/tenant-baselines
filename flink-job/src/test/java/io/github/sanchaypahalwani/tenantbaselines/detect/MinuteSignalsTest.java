package io.github.sanchaypahalwani.tenantbaselines.detect;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import org.junit.jupiter.api.Test;

class MinuteSignalsTest {

  private static MinuteAgg agg(long count, long errors, Double p95) {
    return new MinuteAgg("r", "k", "t", "eu", "u", 0, count, errors, p95);
  }

  @Test
  void belowMinimumEvidenceIsUnscored() {
    var s = MinuteSignals.of(agg(19, 19, 500.0), 100);
    assertThat(s.errorRate()).isNaN();
    assertThat(s.latencyP95()).isNaN();
    assertThat(s.volume5m()).isEqualTo(100);
  }

  @Test
  void errorRateIsFraction() {
    var s = MinuteSignals.of(agg(100, 10, 120.0), Double.NaN);
    assertThat(s.errorRate()).isEqualTo(0.1);
    assertThat(s.latencyP95()).isEqualTo(120.0);
    assertThat(s.get(Signal.ERROR_RATE)).isEqualTo(0.1);
    assertThat(s.get(Signal.VOLUME_5M)).isNaN();
  }
}
