package io.github.sanchaypahalwani.tenantbaselines.detect;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import java.io.InputStream;
import java.util.List;
import org.junit.jupiter.api.Test;

class ThresholdsFileTest {

  @Test
  void loadsDemoFile() throws Exception {
    try (InputStream in = getClass().getResourceAsStream("/thresholds-demo.json")) {
      List<ThresholdSet> sets = ThresholdsFile.load(in);
      assertThat(sets).hasSize(1);
      ThresholdSet s = sets.get(0);
      assertThat(s.detector()).isEqualTo("global_static");
      assertThat(s.setting()).isEqualTo("default");
      assertThat(s.validFromMs()).isZero();
      assertThat(s.validToMs()).isEqualTo(Long.MAX_VALUE);
      assertThat(s.levelFor("any", Signal.ERROR_RATE)).isEqualTo(new Level(0.05, 0.025));
      assertThat(s.levelFor("any", Signal.LATENCY_P95)).isEqualTo(new Level(400, 250));
      assertThat(s.levelFor("any", Signal.VOLUME_5M)).isEqualTo(new Level(0, 5));
    }
  }
}
