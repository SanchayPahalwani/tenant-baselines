package io.github.sanchaypahalwani.tenantbaselines.detect;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.baseline.PopulationBaseline;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

class ScorerTest {

  private static final String KEY = "a|eu|u1";

  private static List<ThresholdSet> thresholds() throws Exception {
    try (InputStream in = ScorerTest.class.getResourceAsStream("/thresholds-demo.json")) {
      return ThresholdsFile.load(in);
    }
  }

  private static Scorer scorer() throws Exception {
    return new Scorer("run", thresholds(), List.of(), false, Duration.ofDays(7));
  }

  private static KeyedBroadcastOperatorTestHarness<
          String, MinuteAgg, PopulationBaseline, AlertEvent>
      harness() throws Exception {
    return ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(
        scorer(), MinuteAgg::key, Types.STRING, Scorer.POPULATION);
  }

  private static void feed(
      KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h,
      int fromMinute,
      int toMinute,
      long count,
      long errors)
      throws Exception {
    for (int m = fromMinute; m <= toMinute; m++) {
      long ts = m * 60_000L;
      var agg = new MinuteAgg("run", KEY, "a", "eu", "u1", ts, count, errors, count == 0 ? null : 100.0);
      h.processElement(agg, ts);
    }
  }

  @Test
  void errorRateFiresAndResolves() throws Exception {
    try (var h = harness()) {
      feed(h, 0, 2, 100, 10);
      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(1);
      AlertEvent fire = out.get(0);
      assertThat(fire.state()).isEqualTo("firing");
      assertThat(fire.signal()).isEqualTo("error_rate");
      assertThat(fire.scope()).isEqualTo("tenant");
      assertThat(fire.alertId())
          .isEqualTo(AlertIds.of("global_static", "default", "tenant", KEY, "error_rate", 0));
      assertThat(fire.eventTimeMs()).isEqualTo(180_000);
      assertThat(fire.startsAtMs()).isZero();
      assertThat(fire.value()).isEqualTo(0.1);
      assertThat(fire.reference()).isEqualTo(0.05);

      feed(h, 3, 7, 100, 0);
      out = h.extractOutputValues();
      assertThat(out).hasSize(2);
      AlertEvent resolved = out.get(1);
      assertThat(resolved.state()).isEqualTo("resolved");
      assertThat(resolved.alertId()).isEqualTo(fire.alertId());
      assertThat(resolved.eventTimeMs()).isEqualTo(8 * 60_000L);
    }
  }

  @Test
  void volumeDownFiresOnSilence() throws Exception {
    try (var h = harness()) {
      feed(h, 0, 4, 10, 0);
      feed(h, 5, 16, 0, 0);
      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(1);
      AlertEvent e = out.get(0);
      assertThat(e.signal()).isEqualTo("volume_5m");
      assertThat(e.state()).isEqualTo("firing");
      assertThat(e.startsAtMs()).isEqualTo(540_000);
      assertThat(e.eventTimeMs()).isEqualTo(720_000);
    }
  }

  @Test
  void scorerSurvivesJavaSerialization() throws Exception {
    Scorer copy = org.apache.flink.util.InstantiationUtil.clone(scorer());
    assertThat(copy).isNotNull();
  }
}
