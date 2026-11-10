package io.github.sanchaypahalwani.tenantbaselines.detect;

import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.BREACH;
import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.CALM;
import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.NEITHER;
import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.UNSCORED;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ZeroTrafficTest {

  @Test
  void zeroSumWithEnoughExpectedIsABreach() {
    assertThat(ZeroTraffic.adjust(CALM, 0, 15)).isEqualTo(BREACH);
    assertThat(ZeroTraffic.adjust(UNSCORED, 0, 20)).isEqualTo(BREACH);
  }

  @Test
  void zeroSumBelowTheMinimumKeepsTheOwnJudgement() {
    assertThat(ZeroTraffic.adjust(CALM, 0, 14.9)).isEqualTo(CALM);
  }

  @Test
  void unscoredKeyWithKnownExpectationResolvesWhenTrafficReturns() {
    assertThat(ZeroTraffic.adjust(UNSCORED, 7, 20)).isEqualTo(CALM);
    assertThat(ZeroTraffic.adjust(UNSCORED, 0, 10)).isEqualTo(NEITHER);
    assertThat(ZeroTraffic.adjust(UNSCORED, 0, Double.NaN)).isEqualTo(UNSCORED);
  }

  @Test
  void windowNotFullLeavesTheJudgementAlone() {
    assertThat(ZeroTraffic.adjust(NEITHER, Double.NaN, 100)).isEqualTo(NEITHER);
  }
}
