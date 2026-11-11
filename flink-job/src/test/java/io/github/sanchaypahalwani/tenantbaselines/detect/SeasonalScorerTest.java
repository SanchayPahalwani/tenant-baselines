package io.github.sanchaypahalwani.tenantbaselines.detect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assertions.within;

import io.github.sanchaypahalwani.tenantbaselines.baseline.HourOfWeek;
import io.github.sanchaypahalwani.tenantbaselines.baseline.PopulationBaseline;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.Keys;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.ScoreEvent;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

class SeasonalScorerTest {

  static final String KEY = "a|eu|u1";
  static final long MINUTE_MS = 60_000L;

  /** Monday 00:00 of week 1, the start of bucket 0. */
  static final long MON = 345_600_000L;

  static final long WEEK = 604_800_000L;

  /** Monday 00:00 of week 3, the first week with two weeks of history behind it. */
  static final long W3 = MON + 2 * WEEK;

  static KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent>
      harness(List<ThresholdSet> sets, boolean emitScores) throws Exception {
    Scorer scorer =
        new Scorer("run", sets, List.of(SeasonalSetting.ofK(4)), emitScores, Duration.ofDays(7));
    return ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(
        scorer, MinuteAgg::key, Types.STRING, Scorer.POPULATION);
  }

  static KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent>
      harness(boolean emitScores) throws Exception {
    return harness(List.of(), emitScores);
  }

  static List<ThresholdSet> demoThresholds() throws Exception {
    try (InputStream in = SeasonalScorerTest.class.getResourceAsStream("/thresholds-test.json")) {
      return ThresholdsFile.load(in);
    }
  }

  static void minute(
      KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h,
      String key,
      long ms,
      long count,
      long errors,
      double p95)
      throws Exception {
    String[] parts = Keys.split(key);
    Double latency = count == 0 ? null : p95;
    h.processElement(
        new MinuteAgg("run", key, parts[0], parts[1], parts[2], ms, count, errors, latency), ms);
  }

  /** One minute of {@link #KEY} per minute, from {@code fromMs} up to but not including toMs. */
  static void history(
      KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h,
      long fromMs,
      long toMs,
      long count,
      long errors,
      double p95)
      throws Exception {
    for (long ms = fromMs; ms < toMs; ms += MINUTE_MS) {
      minute(h, KEY, ms, count, errors, p95);
    }
  }

  static void twoWeeks(
      KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h)
      throws Exception {
    history(h, MON, W3, 100, 0, 100.0);
  }

  static List<ScoreEvent> scores(
      KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h) {
    ConcurrentLinkedQueue<StreamRecord<ScoreEvent>> q = h.getSideOutput(Scorer.SCORES);
    return q == null ? List.of() : q.stream().map(StreamRecord::getValue).toList();
  }

  static ScoreEvent scoreAt(List<ScoreEvent> scores, String key, long ms, String signal) {
    return scores.stream()
        .filter(s -> s.key().equals(key) && s.minuteMs() == ms && s.signal().equals(signal))
        .findFirst()
        .orElseThrow();
  }

  private static List<AlertEvent> errorRateEvents(
      KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h) {
    return h.extractOutputValues().stream().filter(e -> e.signal().equals("error_rate")).toList();
  }

  static boolean nullOrFinite(Double d) {
    return d == null || Double.isFinite(d);
  }

  @Test
  void errorRateFiresAgainstOwnBaseline() throws Exception {
    try (var h = harness(false)) {
      twoWeeks(h);
      history(h, W3, W3 + 3 * MINUTE_MS, 100, 10, 100.0);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(1);
      AlertEvent e = out.get(0);
      assertThat(e.detector()).isEqualTo("seasonal");
      assertThat(e.setting()).isEqualTo("k4");
      assertThat(e.signal()).isEqualTo("error_rate");
      assertThat(e.state()).isEqualTo("firing");
      assertThat(e.startsAtMs()).isEqualTo(W3);
      assertThat(e.eventTimeMs()).isEqualTo(W3 + 180_000);
      assertThat(e.value()).isEqualTo(0.1);
      assertThat(e.reference()).isCloseTo(0.028214, within(1e-5));
      assertThat(e.alertId())
          .isEqualTo(AlertIds.of("seasonal", "k4", "tenant", KEY, "error_rate", W3));
    }
  }

  @Test
  void oneWeekOfHistoryIsNotEnough() throws Exception {
    try (var h = harness(false)) {
      history(h, MON, MON + WEEK, 100, 0, 100.0);
      history(h, MON + WEEK, MON + WEEK + 3 * MINUTE_MS, 100, 10, 100.0);

      assertThat(h.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void weekNeedsThirtyScoredMinutes() throws Exception {
    try (var h = harness(false)) {
      for (long ms = MON; ms < W3; ms += MINUTE_MS) {
        boolean scored = HourOfWeek.bucket(ms) == 0 && HourOfWeek.minuteOfHour(ms) < 29;
        minute(h, KEY, ms, scored ? 100 : 5, 0, 100.0);
      }
      history(h, W3, W3 + 3 * MINUTE_MS, 100, 10, 100.0);

      assertThat(h.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void latencyUsesFivePercentFloor() throws Exception {
    try (var h = harness(false)) {
      twoWeeks(h);
      history(h, W3, W3 + 3 * MINUTE_MS, 100, 0, 120.0);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(1);
      assertThat(out.get(0).signal()).isEqualTo("latency_p95");
      assertThat(out.get(0).state()).isEqualTo("firing");
      assertThat(out.get(0).reference()).isEqualTo(120.0);
    }
    try (var h = harness(false)) {
      twoWeeks(h);
      history(h, W3, W3 + 3 * MINUTE_MS, 100, 0, 119.9);

      assertThat(h.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void volumeDropFiresOnFiveMinuteSum() throws Exception {
    try (var h = harness(false)) {
      twoWeeks(h);
      history(h, W3, W3 + 8 * MINUTE_MS, 80, 0, 100.0);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(1);
      AlertEvent e = out.get(0);
      assertThat(e.signal()).isEqualTo("volume_5m");
      assertThat(e.state()).isEqualTo("firing");
      assertThat(e.startsAtMs()).isEqualTo(W3 + 240_000);
      assertThat(e.eventTimeMs()).isEqualTo(W3 + 420_000);
      assertThat(e.value()).isEqualTo(400);
      assertThat(e.reference()).isCloseTo(410.557, within(0.01));
      assertThat(e.reason()).isNull();
    }
  }

  @Test
  void volumeFireReferenceIsNeverNegative() throws Exception {
    try (var h = harness(false)) {
      // Expected 15 with spread √15, so expected − 4 · spread is below zero.
      history(h, MON, W3, 3, 0, 100.0);
      history(h, W3, W3 + 7 * MINUTE_MS, 0, 0, 100.0);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(1);
      AlertEvent e = out.get(0);
      assertThat(e.signal()).isEqualTo("volume_5m");
      assertThat(e.state()).isEqualTo("firing");
      assertThat(e.startsAtMs()).isEqualTo(W3 + 4 * MINUTE_MS);
      assertThat(e.reason()).isEqualTo("zero_traffic");
      assertThat(e.value()).isZero();
      assertThat(e.reference()).isEqualTo(0.0);
    }
  }

  @Test
  void coldVolumeIsUnscored() throws Exception {
    try (var h = harness(false)) {
      history(h, MON, MON + WEEK, 100, 0, 100.0);
      history(h, MON + WEEK, MON + WEEK + 20 * MINUTE_MS, 30, 0, 100.0);

      assertThat(h.extractOutputValues()).isEmpty();
    }
  }

  private static void zeroTrafficOnColdKey(
      KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h,
      long countBefore)
      throws Exception {
    history(h, MON, MON + 40 * MINUTE_MS, countBefore, 0, 100.0);
    history(h, MON + 40 * MINUTE_MS, MON + 48 * MINUTE_MS, 0, 0, 100.0);
  }

  @Test
  void zeroTrafficFiresOnColdKey() throws Exception {
    try (var h = harness(false)) {
      zeroTrafficOnColdKey(h, 4);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(1);
      AlertEvent e = out.get(0);
      assertThat(e.signal()).isEqualTo("volume_5m");
      assertThat(e.state()).isEqualTo("firing");
      assertThat(e.startsAtMs()).isEqualTo(MON + 44 * MINUTE_MS);
      assertThat(e.reason()).isEqualTo("zero_traffic");
      assertThat(e.value()).isZero();
      assertThat(e.reference()).isEqualTo(0.0);
    }
  }

  @Test
  void zeroTrafficNeedsFifteenExpected() throws Exception {
    try (var h = harness(false)) {
      zeroTrafficOnColdKey(h, 2);

      assertThat(h.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void zeroTrafficAlertResolvesWhenTrafficReturns() throws Exception {
    try (var h = harness(false)) {
      zeroTrafficOnColdKey(h, 4);
      AlertEvent fire = h.extractOutputValues().get(0);

      history(h, MON + 48 * MINUTE_MS, MON + 53 * MINUTE_MS, 4, 0, 100.0);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(2);
      assertThat(out.get(1).state()).isEqualTo("resolved");
      assertThat(out.get(1).alertId()).isEqualTo(fire.alertId());
      assertThat(out.get(1).reason()).isNull();
    }
  }

  @Test
  void unscoredErrorMinutesPauseTheCalmStreak() throws Exception {
    try (var h = harness(false)) {
      twoWeeks(h);
      history(h, W3, W3 + 3 * MINUTE_MS, 100, 10, 100.0);
      history(h, W3 + 3 * MINUTE_MS, W3 + 7 * MINUTE_MS, 100, 0, 100.0);
      // Below minimum evidence: the error rate is unscored, which must not count as calm.
      history(h, W3 + 7 * MINUTE_MS, W3 + 10 * MINUTE_MS, 10, 0, 100.0);
      assertThat(errorRateEvents(h)).extracting(AlertEvent::state).containsExactly("firing");

      history(h, W3 + 10 * MINUTE_MS, W3 + 11 * MINUTE_MS, 100, 0, 100.0);

      List<AlertEvent> out = errorRateEvents(h);
      assertThat(out).extracting(AlertEvent::state).containsExactly("firing", "resolved");
      assertThat(out.get(1).eventTimeMs()).isEqualTo(W3 + 11 * MINUTE_MS);
    }
  }

  @Test
  void scoreIsConstantWithinAnHourOfTheCurrentWeek() throws Exception {
    try (var h = harness(true)) {
      twoWeeks(h);
      history(h, W3, W3 + 40 * MINUTE_MS, 100, 50, 100.0);

      List<ScoreEvent> scores = scores(h);
      Double first = scoreAt(scores, KEY, W3, "error_rate").score();
      Double fortieth = scoreAt(scores, KEY, W3 + 39 * MINUTE_MS, "error_rate").score();
      assertThat(first).isNotNull();
      assertThat(fortieth).isCloseTo(first, within(1e-9));
    }
  }

  @Test
  void minutesWhileFiringStillFeedTheBaseline() throws Exception {
    try (var h = harness(true)) {
      // Weeks 1 and 2: 30 calm, scored minutes of bucket 0, so the key is warm in week 3.
      history(h, MON, MON + 30 * MINUTE_MS, 100, 0, 100.0);
      history(h, MON + WEEK, MON + WEEK + 30 * MINUTE_MS, 100, 0, 100.0);
      // Week 3: the whole hour fires against the zero baseline.
      history(h, W3, W3 + 60 * MINUTE_MS, 100, 50, 100.0);
      assertThat(errorRateEvents(h)).extracting(AlertEvent::state).containsExactly("firing");
      // Week 4: the window is 60 calm values and 60 firing ones.
      history(h, W3 + WEEK, W3 + WEEK + MINUTE_MS, 100, 50, 100.0);

      List<ScoreEvent> scores = scores(h);
      Double inWeek3 = scoreAt(scores, KEY, W3, "error_rate").median();
      Double inWeek4 = scoreAt(scores, KEY, W3 + WEEK, "error_rate").median();
      assertThat(inWeek3).isEqualTo(0.0);
      assertThat(inWeek4).isCloseTo(0.25, within(1e-9));
    }
  }

  @Test
  void scoresAreEmittedPerSignalWithBand() throws Exception {
    String other = "b|eu|u1";
    try (var h = harness(true)) {
      twoWeeks(h);
      history(h, W3, W3 + 3 * MINUTE_MS, 100, 10, 100.0);
      // A cold key whose traffic stops, so the run also has an alert without a baseline.
      for (int m = 0; m < 48; m++) {
        minute(h, other, MON + m * MINUTE_MS, m < 40 ? 4 : 0, 0, 100.0);
      }

      List<ScoreEvent> scores = scores(h);
      Map<String, Set<String>> signalsPerMinute =
          scores.stream()
              .collect(
                  Collectors.groupingBy(
                      s -> s.key() + "@" + s.minuteMs(),
                      Collectors.mapping(ScoreEvent::signal, Collectors.toSet())));
      long minutes = (W3 + 3 * MINUTE_MS - MON) / MINUTE_MS + 48;
      assertThat(scores).hasSize((int) (3 * minutes));
      assertThat(signalsPerMinute)
          .hasSize((int) minutes)
          .allSatisfy(
              (minute, signals) ->
                  assertThat(signals)
                      .containsExactlyInAnyOrder("error_rate", "latency_p95", "volume_5m"));

      ScoreEvent warm = scoreAt(scores, KEY, W3, "error_rate");
      assertThat(warm.median()).isEqualTo(0.0);
      assertThat(warm.spread()).isCloseTo(0.0070534, within(1e-7));
      assertThat(warm.source()).isEqualTo("own");

      for (String signal : List.of("error_rate", "latency_p95")) {
        ScoreEvent cold = scoreAt(scores, KEY, MON, signal);
        assertThat(cold.value()).isNotNull();
        assertThat(cold.median()).isNull();
        assertThat(cold.spread()).isNull();
        assertThat(cold.score()).isNull();
        assertThat(cold.source()).isNull();
      }

      assertThat(scores)
          .allSatisfy(
              s ->
                  assertThat(Stream.of(s.value(), s.median(), s.spread(), s.score()))
                      .allMatch(SeasonalScorerTest::nullOrFinite));
      List<AlertEvent> alerts = h.extractOutputValues();
      assertThat(alerts)
          .extracting(AlertEvent::key, AlertEvent::signal)
          .contains(tuple(KEY, "error_rate"), tuple(other, "volume_5m"));
      assertThat(alerts)
          .allSatisfy(
              a -> {
                assertThat(Double.isFinite(a.value())).isTrue();
                assertThat(Double.isFinite(a.reference())).isTrue();
              });
    }
  }

  @Test
  void noScoresWhenDisabled() throws Exception {
    try (var h = harness(false)) {
      history(h, MON, MON + 10 * MINUTE_MS, 100, 0, 100.0);

      assertThat(h.getSideOutput(Scorer.SCORES)).isNullOrEmpty();
    }
  }

  @Test
  void staticAndSeasonalRunSideBySide() throws Exception {
    try (var h = harness(demoThresholds(), false)) {
      twoWeeks(h);
      history(h, W3, W3 + 3 * MINUTE_MS, 100, 10, 100.0);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(2);
      assertThat(out)
          .allSatisfy(
              e -> {
                assertThat(e.signal()).isEqualTo("error_rate");
                assertThat(e.state()).isEqualTo("firing");
              });
      assertThat(out)
          .extracting(AlertEvent::detector)
          .containsExactlyInAnyOrder("global_static", "seasonal");
      assertThat(out.get(0).alertId()).isNotEqualTo(out.get(1).alertId());
    }
  }
}
