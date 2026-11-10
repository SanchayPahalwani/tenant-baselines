package io.github.sanchaypahalwani.tenantbaselines.detect;

import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.BREACH;
import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.CALM;
import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.NEITHER;
import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.UNSCORED;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import org.junit.jupiter.api.Test;

class LevelTest {

  @Test
  void boundaries() {
    Level up = new Level(0.05, 0.025);
    assertThat(up.judge(Signal.ERROR_RATE, 0.05)).isEqualTo(BREACH);
    assertThat(up.judge(Signal.ERROR_RATE, 0.0499)).isEqualTo(NEITHER);
    assertThat(up.judge(Signal.ERROR_RATE, 0.025)).isEqualTo(NEITHER);
    assertThat(up.judge(Signal.ERROR_RATE, 0.0249)).isEqualTo(CALM);
    assertThat(up.judge(Signal.ERROR_RATE, Double.NaN)).isEqualTo(UNSCORED);

    Level down = new Level(0, 5);
    assertThat(down.judge(Signal.VOLUME_5M, 0)).isEqualTo(BREACH);
    assertThat(down.judge(Signal.VOLUME_5M, 5)).isEqualTo(NEITHER);
    assertThat(down.judge(Signal.VOLUME_5M, 5.1)).isEqualTo(CALM);
    assertThat(down.judge(Signal.VOLUME_5M, Double.NaN)).isEqualTo(UNSCORED);
  }
}
