package io.github.sanchaypahalwani.tenantbaselines.detect;

import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.KEY;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.MINUTE_MS;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.MON;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.W3;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.WEEK;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.harness;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.history;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.minute;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.scoreAt;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.scores;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.twoWeeks;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.github.sanchaypahalwani.tenantbaselines.agg.MinuteAccumulator;
import io.github.sanchaypahalwani.tenantbaselines.baseline.BucketHistory;
import io.github.sanchaypahalwani.tenantbaselines.baseline.HourCache;
import io.github.sanchaypahalwani.tenantbaselines.baseline.PopHistogram;
import io.github.sanchaypahalwani.tenantbaselines.baseline.PopulationBaseline;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.ScoreEvent;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.junit.jupiter.api.Test;

class ScorerFallbackTest {

  /** A key of the same region as {@link SeasonalScorerTest#KEY} that has no history. */
  private static final String COLD = "b|eu|u1";

  /** Bucket 0 of eu: error rate 1 % (scaled MAD 0.002), latency p95 100 ms (scaled MAD 4). */
  private static PopulationBaseline region(long week) {
    return new PopulationBaseline("eu", 0, week, 0.01, 0.002, 100, 4, 500);
  }

  /** The same numbers as {@link #region} for the global scope. */
  private static PopulationBaseline global(long week) {
    return new PopulationBaseline(PopulationBaseline.GLOBAL, 0, week, 0.01, 0.002, 100, 4, 500);
  }

  /** Three minutes of a 10 % error rate from fromMs. */
  private static void errorSpike(
      KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h,
      String key,
      long fromMs)
      throws Exception {
    for (int m = 0; m < 3; m++) {
      minute(h, key, fromMs + m * MINUTE_MS, 100, 10, 100.0);
    }
  }

  private static List<ScoreEvent> scoresOf(
      KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h,
      String signal) {
    return scores(h).stream().filter(s -> s.signal().equals(signal)).toList();
  }

  private static void assertOneSeasonalFiring(List<AlertEvent> out, String signal) {
    assertThat(out).hasSize(1);
    AlertEvent e = out.get(0);
    assertThat(e.detector()).isEqualTo("seasonal");
    assertThat(e.signal()).isEqualTo(signal);
    assertThat(e.state()).isEqualTo("firing");
  }

  @Test
  void coldKeyIsUnscoredWithoutFallback() throws Exception {
    try (var h = harness(true)) {
      errorSpike(h, KEY, MON);

      assertThat(h.extractOutputValues()).isEmpty();
      assertThat(scoresOf(h, "error_rate"))
          .hasSize(3)
          .allSatisfy(
              s -> {
                assertThat(s.median()).isNull();
                assertThat(s.score()).isNull();
                assertThat(s.source()).isNull();
              });
    }
  }

  @Test
  void fallsBackToRegion() throws Exception {
    try (var h = harness(true)) {
      h.processBroadcastElement(region(1), MON);
      errorSpike(h, KEY, MON);

      assertOneSeasonalFiring(h.extractOutputValues(), "error_rate");
      ScoreEvent s = scoreAt(scores(h), KEY, MON, "error_rate");
      assertThat(s.median()).isEqualTo(0.01);
      assertThat(s.score()).isCloseTo(9.045, within(0.01));
      assertThat(s.source()).isEqualTo("region");
    }
  }

  @Test
  void fallsBackToGlobalWithoutRegion() throws Exception {
    try (var h = harness(true)) {
      h.processBroadcastElement(global(1), MON);
      errorSpike(h, KEY, MON);

      assertOneSeasonalFiring(h.extractOutputValues(), "error_rate");
      assertThat(scoreAt(scores(h), KEY, MON, "error_rate").source()).isEqualTo("global");
    }
    try (var h = harness(true)) {
      h.processBroadcastElement(global(1), MON);
      h.processBroadcastElement(region(1), MON);
      errorSpike(h, KEY, MON);

      assertOneSeasonalFiring(h.extractOutputValues(), "error_rate");
      assertThat(scoreAt(scores(h), KEY, MON, "error_rate").source()).isEqualTo("region");
    }
  }

  @Test
  void latencyFallsBackToo() throws Exception {
    try (var h = harness(true)) {
      h.processBroadcastElement(region(1), MON);
      history(h, MON, MON + 3 * MINUTE_MS, 100, 0, 120.0);

      List<AlertEvent> out = h.extractOutputValues();
      assertOneSeasonalFiring(out, "latency_p95");
      assertThat(out.get(0).reference()).isEqualTo(120.0);
      ScoreEvent s = scoreAt(scores(h), KEY, MON, "latency_p95");
      assertThat(s.spread()).isEqualTo(5.0);
      assertThat(s.score()).isEqualTo(4.0);
      assertThat(s.source()).isEqualTo("region");
    }
  }

  @Test
  void fallbackForNextWeekIsNotUsedEarly() throws Exception {
    try (var h = harness(true)) {
      h.processBroadcastElement(region(2), MON);
      errorSpike(h, KEY, MON);
      assertThat(h.extractOutputValues()).isEmpty();

      h.processBroadcastElement(region(1), MON);
      errorSpike(h, KEY, MON + 10 * MINUTE_MS);

      assertOneSeasonalFiring(h.extractOutputValues(), "error_rate");
    }
  }

  @Test
  void olderFallbackIsUsedWhenThisWeeksIsMissing() throws Exception {
    try (var h = harness(true)) {
      h.processBroadcastElement(region(1), MON);
      errorSpike(h, KEY, MON + 2 * WEEK);

      assertOneSeasonalFiring(h.extractOutputValues(), "error_rate");
      assertThat(scoreAt(scores(h), KEY, MON + 2 * WEEK, "error_rate").source())
          .isEqualTo("region");
    }
  }

  @Test
  void ownBaselineWinsOnceWarm() throws Exception {
    try (var h = harness(true)) {
      // Each week's record is published before the week starts, as PopulationBaselines does.
      h.processBroadcastElement(region(1), MON);
      h.processBroadcastElement(region(2), MON);
      history(h, MON, MON + WEEK, 100, 0, 100.0);
      h.processBroadcastElement(region(3), MON + WEEK);
      history(h, MON + WEEK, W3, 100, 0, 100.0);
      minute(h, KEY, W3, 100, 0, 100.0);

      List<ScoreEvent> scores = scores(h);
      assertThat(scoreAt(scores, KEY, MON, "error_rate").source()).isEqualTo("region");
      ScoreEvent warm = scoreAt(scores, KEY, W3, "error_rate");
      assertThat(warm.source()).isEqualTo("own");
      assertThat(warm.median()).isEqualTo(0.0);
    }
  }

  @Test
  void volumeNeverUsesFallback() throws Exception {
    try (var h = harness(true)) {
      h.processBroadcastElement(region(1), MON);
      history(h, MON, MON + 10 * MINUTE_MS, 100, 0, 100.0);
      history(h, MON + 10 * MINUTE_MS, MON + 20 * MINUTE_MS, 30, 0, 100.0);

      assertThat(h.extractOutputValues())
          .extracting(AlertEvent::signal)
          .doesNotContain("volume_5m");
      assertThat(scoresOf(h, "volume_5m"))
          .hasSize(20)
          .allSatisfy(
              s -> {
                assertThat(s.median()).isNull();
                assertThat(s.source()).isNull();
              });
    }
  }

  @Test
  void baselinesSurviveSnapshotAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (var h = harness(true)) {
      twoWeeks(h);
      h.processBroadcastElement(region(3), MON + WEEK);
      snapshot = h.snapshot(1, 1);
    }

    try (var h = restored(snapshot)) {
      // KEY's expiry timer came back with the snapshot; COLD gets its own on its first minute.
      // KEY already has its timer, so its minutes add none: 2 means one restored and one new.
      assertThat(h.numEventTimeTimers()).isEqualTo(1);
      errorSpike(h, KEY, W3);
      errorSpike(h, COLD, W3);
      assertThat(h.numEventTimeTimers()).isEqualTo(2);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(2);
      AlertEvent own = out.stream().filter(e -> e.key().equals(KEY)).findFirst().orElseThrow();
      // The same alert as SeasonalScorerTest.errorRateFiresAgainstOwnBaseline, which has no
      // snapshot in between.
      assertThat(own.alertId())
          .isEqualTo(AlertIds.of("seasonal", "k4", "tenant", KEY, "error_rate", W3));
      assertThat(own.reference()).isCloseTo(0.028214, within(1e-5));
      AlertEvent cold = out.stream().filter(e -> e.key().equals(COLD)).findFirst().orElseThrow();
      assertThat(cold.signal()).isEqualTo("error_rate");
      assertThat(cold.state()).isEqualTo("firing");
      assertThat(scoreAt(scores(h), COLD, W3, "error_rate").source()).isEqualTo("region");
    }
  }

  /** A fresh Scorer harness started from a snapshot, set up like SeasonalScorerTest's. */
  private static KeyedBroadcastOperatorTestHarness<
          String, MinuteAgg, PopulationBaseline, AlertEvent>
      restored(OperatorSubtaskState snapshot) throws Exception {
    Scorer scorer =
        new Scorer("run", List.of(), List.of(SeasonalSetting.ofK(4)), true, Duration.ofDays(7));
    KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h =
        new KeyedBroadcastOperatorTestHarness<>(
            new CoBroadcastWithKeyedOperator<>(
                scorer, List.<MapStateDescriptor<?, ?>>of(Scorer.POPULATION)),
            MinuteAgg::key,
            Types.STRING,
            1,
            1,
            0);
    h.initializeState(snapshot);
    h.open();
    return h;
  }

  @Test
  void stateTypesArePojos() {
    for (Class<?> c :
        List.of(
            BucketHistory.class,
            HourCache.class,
            VolumeRing.class,
            AlertStateMachine.class,
            PopHistogram.class,
            PopulationBaseline.class,
            MinuteAccumulator.class)) {
      assertThat(TypeInformation.of(c)).as(c.getSimpleName()).isInstanceOf(PojoTypeInfo.class);
    }
  }
}
