package io.github.sanchaypahalwani.tenantbaselines.detect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.github.sanchaypahalwani.tenantbaselines.baseline.PopulationBaseline;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

class ScorerTest {

  private static final String KEY = "a|eu|u1";

  /** Monday 00:00 of week 1, as a minute number for {@link #feed}. */
  private static final int MON_MINUTE = (int) (SeasonalScorerTest.MON / 60_000L);

  private static List<ThresholdSet> thresholds() throws Exception {
    try (InputStream in = ScorerTest.class.getResourceAsStream("/thresholds-test.json")) {
      return ThresholdsFile.load(in);
    }
  }

  private static Scorer scorer() throws Exception {
    return scorer(thresholds());
  }

  private static Scorer scorer(List<ThresholdSet> sets) {
    return new Scorer("run", sets, List.of(), false, Duration.ofDays(7));
  }

  private static KeyedBroadcastOperatorTestHarness<
          String, MinuteAgg, PopulationBaseline, AlertEvent>
      harness(List<ThresholdSet> sets) throws Exception {
    return ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(
        scorer(sets), MinuteAgg::key, Types.STRING, Scorer.POPULATION);
  }

  private static KeyedBroadcastOperatorTestHarness<
          String, MinuteAgg, PopulationBaseline, AlertEvent>
      harness() throws Exception {
    return harness(thresholds());
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

  /** One global_static/p99 set for [fromMs, toMs) with an error-rate level only; null is open. */
  private static String errorRateSet(long fromMs, Long toMs) {
    return """
        {"detector": "global_static", "setting": "p99", "valid_from_ms": %d, "valid_to_ms": %s,
         "global": {"error_rate": {"fire": 0.05, "resolve": 0.025}}, "per_key": {}}"""
        .formatted(fromMs, toMs);
  }

  /** The sets, read through the loader so that they pass its checks. */
  private static List<ThresholdSet> load(String... sets) throws Exception {
    String json = "{\"sets\": [" + String.join(", ", sets) + "]}";
    return ThresholdsFile.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
  }

  /** Two global_static/p99 sets with a gap: [0, MON + 10 min) and [MON + 12 min, open). */
  private static List<ThresholdSet> setsWithAGap() throws Exception {
    return load(errorRateSet(0, ms(MON_MINUTE + 10)), errorRateSet(ms(MON_MINUTE + 12), null));
  }

  /** The start of minute number m. */
  private static long ms(int m) {
    return m * 60_000L;
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
  void zeroTrafficFiresWithoutAVolumeLevel() throws Exception {
    ThresholdSet noVolume =
        new ThresholdSet(
            "global_static",
            "default",
            0,
            Long.MAX_VALUE,
            Map.of(
                Signal.ERROR_RATE, new Level(0.05, 0.025), Signal.LATENCY_P95, new Level(400, 250)),
            Map.of());
    try (var h = harness(List.of(noVolume))) {
      feed(h, 0, 39, 4, 0);
      feed(h, 40, 47, 0, 0);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(1);
      AlertEvent e = out.get(0);
      assertThat(e.detector()).isEqualTo("global_static");
      assertThat(e.signal()).isEqualTo("volume_5m");
      assertThat(e.state()).isEqualTo("firing");
      assertThat(e.startsAtMs()).isEqualTo(44 * 60_000L);
      assertThat(e.reason()).isEqualTo("zero_traffic");
      assertThat(e.value()).isZero();
      assertThat(e.reference()).isEqualTo(0.0);
    }
  }

  @Test
  void firingAlertClosesWhenItsSetEnds() throws Exception {
    try (var h = harness(load(errorRateSet(0, ms(MON_MINUTE + 10))))) {
      feed(h, MON_MINUTE, MON_MINUTE + 9, 100, 20);
      assertThat(h.extractOutputValues()).extracting(AlertEvent::state).containsExactly("firing");

      // The set's last minute is MON + 9 min, so the minute MON + 10 min closes the alert.
      feed(h, MON_MINUTE + 10, MON_MINUTE + 10, 100, 20);
      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(2);
      AlertEvent fire = out.get(0);
      assertThat(fire.state()).isEqualTo("firing");
      assertThat(fire.startsAtMs()).isEqualTo(ms(MON_MINUTE));
      AlertEvent closed = out.get(1);
      assertThat(closed.state()).isEqualTo("resolved");
      assertThat(closed.reason()).isEqualTo("thresholds_expired");
      assertThat(closed.eventTimeMs()).isEqualTo(ms(MON_MINUTE + 11));
      assertThat(closed.alertId()).isEqualTo(fire.alertId());
      assertThat(closed.startsAtMs()).isEqualTo(fire.startsAtMs());
      assertThat(closed)
          .extracting(
              AlertEvent::detector,
              AlertEvent::setting,
              AlertEvent::scope,
              AlertEvent::signal,
              AlertEvent::key,
              AlertEvent::tenant,
              AlertEvent::region,
              AlertEvent::unit)
          .containsExactly(
              fire.detector(),
              fire.setting(),
              fire.scope(),
              fire.signal(),
              fire.key(),
              fire.tenant(),
              fire.region(),
              fire.unit());
      assertThat(closed.value()).isEqualTo(0.0);
      assertThat(closed.reference()).isEqualTo(0.0);

      feed(h, MON_MINUTE + 11, MON_MINUTE + 12, 100, 20);
      assertThat(h.extractOutputValues()).hasSize(2);
    }
  }

  @Test
  void alertCarriesIntoTheNextSetOfTheSamePair() throws Exception {
    List<ThresholdSet> sets =
        load(errorRateSet(0, ms(MON_MINUTE + 10)), errorRateSet(ms(MON_MINUTE + 10), null));
    try (var h = harness(sets)) {
      feed(h, MON_MINUTE, MON_MINUTE + 12, 100, 20);

      assertThat(h.extractOutputValues())
          .extracting(AlertEvent::setting, AlertEvent::signal, AlertEvent::state)
          .containsExactly(tuple("p99", "error_rate", "firing"));
    }
  }

  @Test
  void streakDoesNotCrossAGapBetweenSets() throws Exception {
    try (var h = harness(setsWithAGap())) {
      feed(h, MON_MINUTE, MON_MINUTE + 7, 100, 0);
      // Two breaches end the first set. Nothing judges the gap's minutes.
      feed(h, MON_MINUTE + 8, MON_MINUTE + 13, 100, 20);
      assertThat(h.extractOutputValues()).isEmpty();

      // The second set's third breach (FIRE_AFTER is 3) fires, at MON + 14 min.
      feed(h, MON_MINUTE + 14, MON_MINUTE + 14, 100, 20);
      assertThat(h.extractOutputValues())
          .extracting(AlertEvent::state, AlertEvent::startsAtMs, AlertEvent::eventTimeMs)
          .containsExactly(tuple("firing", ms(MON_MINUTE + 12), ms(MON_MINUTE + 15)));
    }
  }

  @Test
  void alertInTheSetAfterAGapIsANewAlert() throws Exception {
    try (var h = harness(setsWithAGap())) {
      feed(h, MON_MINUTE, MON_MINUTE + 16, 100, 20);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out)
          .extracting(
              AlertEvent::state,
              AlertEvent::startsAtMs,
              AlertEvent::eventTimeMs,
              AlertEvent::reason)
          .containsExactly(
              tuple("firing", ms(MON_MINUTE), ms(MON_MINUTE + 3), null),
              tuple("resolved", ms(MON_MINUTE), ms(MON_MINUTE + 11), "thresholds_expired"),
              tuple("firing", ms(MON_MINUTE + 12), ms(MON_MINUTE + 15), null));
      assertThat(out.get(1).alertId()).isEqualTo(out.get(0).alertId());
      assertThat(out.get(2).alertId())
          .isEqualTo(
              AlertIds.of(
                  "global_static", "p99", "tenant", KEY, "error_rate", ms(MON_MINUTE + 12)))
          .isNotEqualTo(out.get(0).alertId());
    }
  }

  @Test
  void scorerSurvivesJavaSerialization() throws Exception {
    Scorer copy = org.apache.flink.util.InstantiationUtil.clone(scorer());
    assertThat(copy).isNotNull();
  }
}
