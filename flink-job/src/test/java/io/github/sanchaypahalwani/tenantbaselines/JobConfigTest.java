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
    assertThat(cfg.unitStatusTopic()).isEqualTo("live.unit-status");
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
  void aggregatesModeNeedsATopicAndAnEnd() {
    String[] noTopic = {"--mode", "aggregates", "--replay-end-ms", "600000"};
    assertThatThrownBy(() -> JobConfig.parse(noTopic))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--input-topic");
    String[] noEnd = {"--mode", "aggregates", "--input-topic", "x.aggregates-in"};
    assertThatThrownBy(() -> JobConfig.parse(noEnd))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--replay-end-ms");
  }

  @Test
  void aggregatesModeIsBounded() {
    JobConfig cfg =
        JobConfig.parse(
            new String[] {
              "--ns", "eval", "--mode", "aggregates", "--input-topic", "x.aggregates-in",
              "--replay-end-ms", "600000"
            });

    assertThat(cfg.mode()).isEqualTo("aggregates");
    assertThat(cfg.bounded()).isTrue();
    assertThat(cfg.inputTopic()).isEqualTo("x.aggregates-in");
    assertThat(cfg.replayEndMs()).isEqualTo(600_000L);
  }

  @Test
  void liveSettingDefaultsToTheFirstK() {
    assertThat(JobConfig.parse(new String[] {"--seasonal-k", "3,4"}).liveSetting())
        .isEqualTo("k3");
    // The demo passes no --seasonal-k: it keeps following k4.
    assertThat(JobConfig.parse(new String[0]).liveSetting()).isEqualTo("k4");
    String[] named = {"--seasonal-k", "3,4,5,6", "--live-setting", "k4"};
    assertThat(JobConfig.parse(named).liveSetting()).isEqualTo("k4");
  }

  @Test
  void liveSettingMustBeASeasonalSetting() {
    assertThatThrownBy(
            () -> JobConfig.parse(new String[] {"--seasonal-k", "3,4", "--live-setting", "k5"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--live-setting must be one of");
  }

  @Test
  void inputTopicDefaultsToTheNamespace() {
    assertThat(JobConfig.parse(new String[] {"--ns", "eval"}).inputTopic())
        .isEqualTo("eval.events");
    String[] replay = {"--ns", "eval", "--mode", "replay", "--input-topic", "r1.events-in"};
    assertThat(JobConfig.parse(replay).inputTopic()).isEqualTo("r1.events-in");
  }

  @Test
  void scoresFlagTakesOnlyTrueOrFalse() {
    assertThat(JobConfig.parse(new String[] {"--scores", "false"}).emitScores()).isFalse();
    assertThatThrownBy(() -> JobConfig.parse(new String[] {"--scores", "yes"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--scores");
  }
}
