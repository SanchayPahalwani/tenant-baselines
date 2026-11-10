package io.github.sanchaypahalwani.tenantbaselines;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalSetting;
import java.util.List;
import org.junit.jupiter.api.Test;

class JobConfigTest {

  @Test
  void defaults() {
    JobConfig cfg = JobConfig.parse(new String[0]);

    assertThat(cfg.seasonal()).isEqualTo(List.of(new SeasonalSetting("k4", 4, 2)));
    assertThat(cfg.emitScores()).isTrue();
    assertThat(cfg.scoresTopic()).isEqualTo("live.scores");
  }

  @Test
  void seasonalGridAndScoresFlag() {
    String[] grid = {"--ns", "eval", "--seasonal-k", "3,4,5,6"};
    JobConfig cfg = JobConfig.parse(grid);

    assertThat(cfg.seasonal())
        .extracting(SeasonalSetting::setting)
        .containsExactly("k3", "k4", "k5", "k6");
    assertThat(cfg.seasonal())
        .extracting(SeasonalSetting::resolveK)
        .containsExactly(1.5, 2.0, 2.5, 3.0);
    assertThat(cfg.emitScores()).isFalse();
    assertThat(cfg.scoresTopic()).isEqualTo("eval.scores");

    String[] withScores = {"--ns", "eval", "--seasonal-k", "3,4,5,6", "--scores", "true"};
    assertThat(JobConfig.parse(withScores).emitScores()).isTrue();
  }

  @Test
  void seasonalKMustBePositiveAndNotRepeated() {
    assertThatThrownBy(() -> JobConfig.parse(new String[] {"--seasonal-k", "4,0"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("positive");
    assertThatThrownBy(() -> JobConfig.parse(new String[] {"--seasonal-k", "4,4.0"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("repeats");
  }

  @Test
  void scoresFlagTakesOnlyTrueOrFalse() {
    assertThat(JobConfig.parse(new String[] {"--scores", "false"}).emitScores()).isFalse();
    assertThatThrownBy(() -> JobConfig.parse(new String[] {"--scores", "yes"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--scores");
  }
}
