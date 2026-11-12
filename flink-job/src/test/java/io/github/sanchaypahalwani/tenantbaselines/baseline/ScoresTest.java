package io.github.sanchaypahalwani.tenantbaselines.baseline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.github.sanchaypahalwani.tenantbaselines.detect.Judgement;
import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import org.junit.jupiter.api.Test;

class ScoresTest {

  @Test
  void errorRateSamplingFloor() {
    double spread = Scores.errorRateSpread(0, 0, 37);
    assertThat(spread).isCloseTo(0.011596, within(1e-6));
    assertThat(Scores.score(Signal.ERROR_RATE, 1.0 / 37, 0, spread)).isCloseTo(2.33, within(0.01));
    assertThat(Scores.score(Signal.ERROR_RATE, 2.0 / 37, 0, spread)).isCloseTo(4.66, within(0.01));
  }

  @Test
  void errorRateUsesMedianAboveHalfPercent() {
    assertThat(Scores.errorRateSpread(0.02, 0.001, 100)).isCloseTo(0.014, within(1e-9));
    assertThat(Scores.errorRateSpread(0.02, 0.05, 100)).isEqualTo(0.05);
  }

  @Test
  void latencyFivePercentFloor() {
    assertThat(Scores.latencySpread(100, 2)).isEqualTo(5.0);
    assertThat(Scores.latencySpread(100, 8)).isEqualTo(8.0);
  }

  @Test
  void volumeSquareRootFloor() {
    assertThat(Scores.volumeSpread(3, 100)).isEqualTo(10.0);
    assertThat(Scores.volumeSpread(20, 100)).isEqualTo(20.0);
    assertThat(Scores.score(Signal.VOLUME_5M, 60, 100, 10)).isEqualTo(4.0);
  }

  @Test
  void zeroSpreadIsUnscored() {
    assertThat(Scores.score(Signal.LATENCY_P95, 5, 0, 0)).isNaN();
  }

  @Test
  void fireReferenceUpAndDown() {
    assertThat(Scores.fireReference(Signal.ERROR_RATE, 10, 2, 4)).isEqualTo(18.0);
    assertThat(Scores.fireReference(Signal.VOLUME_5M, 10, 2, 4)).isEqualTo(2.0);
  }

  @Test
  void downScoreIsNegativeAboveTheReference() {
    assertThat(Scores.score(Signal.VOLUME_5M, 14, 10, 2)).isEqualTo(-2.0);
  }

  @Test
  void nanInputsAreUnscored() {
    double nan = Double.NaN;
    assertThat(Scores.score(Signal.ERROR_RATE, nan, 1, 1)).isNaN();
    assertThat(Scores.score(Signal.ERROR_RATE, 1, nan, 1)).isNaN();
    assertThat(Scores.score(Signal.ERROR_RATE, 1, 1, nan)).isNaN();
    // A zero or negative spread is unscored too. 2 - 1 over 0 would be +Infinity, not NaN, so
    // only the guard makes these NaN.
    assertThat(Scores.score(Signal.ERROR_RATE, 2, 1, 0)).isNaN();
    assertThat(Scores.score(Signal.ERROR_RATE, 2, 1, -1)).isNaN();
    assertThat(Scores.judge(nan, 4, 2)).isEqualTo(Judgement.UNSCORED);
  }

  @Test
  void judgeBoundaries() {
    assertThat(Scores.judge(4.0, 4, 2)).isEqualTo(Judgement.BREACH);
    assertThat(Scores.judge(3.999, 4, 2)).isEqualTo(Judgement.NEITHER);
    assertThat(Scores.judge(2.0, 4, 2)).isEqualTo(Judgement.NEITHER);
    assertThat(Scores.judge(1.999, 4, 2)).isEqualTo(Judgement.CALM);
    assertThat(Scores.judge(-3, 4, 2)).isEqualTo(Judgement.CALM);
    assertThat(Scores.judge(Double.NaN, 4, 2)).isEqualTo(Judgement.UNSCORED);
  }
}
