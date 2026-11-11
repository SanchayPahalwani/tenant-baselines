package io.github.sanchaypahalwani.tenantbaselines.detect;

import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.KEY;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.MINUTE_MS;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.demoThresholds;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.history;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.minute;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.scoreAt;
import static io.github.sanchaypahalwani.tenantbaselines.detect.SeasonalScorerTest.scores;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.github.sanchaypahalwani.tenantbaselines.baseline.HourOfWeek;
import io.github.sanchaypahalwani.tenantbaselines.baseline.PopulationBaseline;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.ScoreEvent;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class ScorerExpiryTest {

  private static final long DAY_MS = 86_400_000L;

  /** Minute 2, the key's last active minute, plus 7 days and the 2-minute slack. */
  private static final long EXPIRES_MS = 605_040_000L;

  private static KeyedBroadcastOperatorTestHarness<
          String, MinuteAgg, PopulationBaseline, AlertEvent>
      harness() throws Exception {
    return SeasonalScorerTest.harness(demoThresholds(), true);
  }

  /** A harness whose Scorer knows the replay's end. */
  private static KeyedBroadcastOperatorTestHarness<
          String, MinuteAgg, PopulationBaseline, AlertEvent>
      harness(long replayEndMs) throws Exception {
    Scorer scorer =
        new Scorer(
            "run",
            demoThresholds(),
            List.of(SeasonalSetting.ofK(4)),
            true,
            Duration.ofDays(7),
            replayEndMs);
    return ProcessFunctionTestHarnesses.forKeyedBroadcastProcessFunction(
        scorer, MinuteAgg::key, Types.STRING, Scorer.POPULATION);
  }

  /** Minutes 0, 1 and 2 at a 10 % error rate, which fires the static error-rate alert. */
  private static AlertEvent errorSpike(
      KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h)
      throws Exception {
    history(h, 0, 3 * MINUTE_MS, 100, 10, 100.0);
    List<AlertEvent> out = h.extractOutputValues();
    assertThat(out)
        .extracting(AlertEvent::detector, AlertEvent::signal, AlertEvent::state)
        .containsExactly(tuple("global_static", "error_rate", "firing"));
    return out.get(0);
  }

  private static List<AlertEvent> expired(
      KeyedBroadcastOperatorTestHarness<String, MinuteAgg, PopulationBaseline, AlertEvent> h) {
    return h.extractOutputValues().stream().filter(e -> "expired".equals(e.reason())).toList();
  }

  private static void assertFinite(List<AlertEvent> events) {
    assertThat(events)
        .allSatisfy(
            e -> {
              assertThat(Double.isFinite(e.value())).as("value of %s", e).isTrue();
              assertThat(Double.isFinite(e.reference())).as("reference of %s", e).isTrue();
            });
  }

  @Test
  void expiresAfterSevenDaysAndResolvesFiringAlerts() throws Exception {
    try (var h = harness()) {
      AlertEvent fire = errorSpike(h);

      h.watermark(604_919_999L);
      assertThat(h.extractOutputValues()).hasSize(1);

      h.watermark(EXPIRES_MS);
      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out).hasSize(2);
      AlertEvent e = out.get(1);
      assertThat(e.state()).isEqualTo("resolved");
      assertThat(e.reason()).isEqualTo("expired");
      assertThat(e.eventTimeMs()).isEqualTo(EXPIRES_MS);
      assertThat(e.alertId()).isEqualTo(fire.alertId());
      assertThat(e.startsAtMs()).isEqualTo(fire.startsAtMs());
      assertThat(e)
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
      assertThat(e.value()).isEqualTo(0.0);
      assertThat(e.reference()).isEqualTo(0.0);
      assertFinite(out);
      assertThat(h.numEventTimeTimers()).isZero();

      // The key starts over: the 5-minute sum needs five new minutes and the alert fires anew.
      history(h, EXPIRES_MS, EXPIRES_MS + 5 * MINUTE_MS, 100, 10, 100.0);
      List<ScoreEvent> scores = scores(h);
      for (int m = 0; m < 4; m++) {
        assertThat(scoreAt(scores, KEY, EXPIRES_MS + m * MINUTE_MS, "volume_5m").value()).isNull();
      }
      assertThat(scoreAt(scores, KEY, EXPIRES_MS + 4 * MINUTE_MS, "volume_5m").value())
          .isEqualTo(500.0);
      out = h.extractOutputValues();
      assertThat(out).hasSize(3);
      assertThat(out.get(2).state()).isEqualTo("firing");
      assertThat(out.get(2).startsAtMs()).isEqualTo(EXPIRES_MS);
      assertThat(h.numEventTimeTimers()).isEqualTo(1);
    }
  }

  @Test
  void zeroMinutesDoNotPostponeExpiry() throws Exception {
    try (var h = harness()) {
      errorSpike(h);
      history(h, 3 * MINUTE_MS, 10_080 * MINUTE_MS, 0, 0, 100.0);
      List<AlertEvent> fired = h.extractOutputValues();
      // The silence raises the static volume alert (its level fires at 0) and the seasonal
      // zero-traffic alert (the trailing expectation is 50 at minute 34).
      assertThat(fired)
          .extracting(AlertEvent::detector, AlertEvent::signal, AlertEvent::state)
          .containsExactlyInAnyOrder(
              tuple("global_static", "error_rate", "firing"),
              tuple("global_static", "volume_5m", "firing"),
              tuple("seasonal", "volume_5m", "firing"));

      h.watermark(EXPIRES_MS);

      List<AlertEvent> expired = expired(h);
      assertThat(expired)
          .extracting(AlertEvent::alertId)
          .containsExactlyInAnyOrderElementsOf(fired.stream().map(AlertEvent::alertId).toList());
      assertThat(expired)
          .allSatisfy(
              e -> {
                assertThat(e.state()).isEqualTo("resolved");
                assertThat(e.eventTimeMs()).isEqualTo(EXPIRES_MS);
              });
      assertThat(h.extractOutputValues()).hasSize(fired.size() + expired.size());
    }
  }

  @Test
  void activityPostponesExpiry() throws Exception {
    try (var h = harness()) {
      errorSpike(h);
      minute(h, KEY, 6 * DAY_MS, 1, 0, 100.0);

      h.watermark(EXPIRES_MS);
      assertThat(expired(h)).isEmpty();
      assertThat(h.numEventTimeTimers()).isEqualTo(1);

      long due = 6 * DAY_MS + 7 * DAY_MS + 120_000L;
      h.watermark(due - 1);
      assertThat(expired(h)).isEmpty();
      h.watermark(due);
      assertThat(expired(h)).extracting(AlertEvent::eventTimeMs).containsExactly(due);
    }
  }

  @Test
  @Timeout(value = 2, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void doesNotExpireAtFinalWatermark() throws Exception {
    try (var h = harness()) {
      errorSpike(h);

      h.watermark(Long.MAX_VALUE);

      assertThat(h.extractOutputValues()).extracting(AlertEvent::state).containsExactly("firing");
    }
  }

  @Test
  void expiresAtTheFinalWatermarkWhenDueBeforeReplayEnd() throws Exception {
    try (var h = harness(EXPIRES_MS + 60_000L)) {
      AlertEvent fire = errorSpike(h);

      // The last real watermark stops 1 ms short of the due time; the final one reaches it.
      h.watermark(EXPIRES_MS - 1);
      assertThat(expired(h)).isEmpty();
      h.watermark(Long.MAX_VALUE);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out)
          .extracting(AlertEvent::state, AlertEvent::reason)
          .containsExactly(tuple("firing", null), tuple("resolved", "expired"));
      AlertEvent e = out.get(1);
      assertThat(e.eventTimeMs()).isEqualTo(EXPIRES_MS);
      assertThat(e.alertId()).isEqualTo(fire.alertId());
    }
  }

  @Test
  void expiresAtTheFinalWatermarkWhenDueIsTheReplayEnd() throws Exception {
    try (var h = harness(EXPIRES_MS)) {
      errorSpike(h);

      h.watermark(EXPIRES_MS - 1);
      h.watermark(Long.MAX_VALUE);

      assertThat(expired(h))
          .extracting(AlertEvent::state, AlertEvent::eventTimeMs)
          .containsExactly(tuple("resolved", EXPIRES_MS));
    }
  }

  @Test
  void doesNotExpireAtFinalWatermarkPastReplayEnd() throws Exception {
    try (var h = harness(EXPIRES_MS - 1)) {
      errorSpike(h);

      h.watermark(EXPIRES_MS - 1);
      h.watermark(Long.MAX_VALUE);

      assertThat(h.extractOutputValues()).extracting(AlertEvent::state).containsExactly("firing");
    }
  }

  @Test
  void finalWatermarkTerminates() {
    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> {
          try (var h = harness(EXPIRES_MS + 60_000L)) {
            // The timer was set at minute 0; minutes 1 and 2 moved the due time on to EXPIRES_MS.
            // So the final watermark finds the timer early, sets a new one at the due time and
            // fires that one too.
            errorSpike(h);

            h.watermark(Long.MAX_VALUE);

            assertThat(expired(h))
                .extracting(AlertEvent::state, AlertEvent::eventTimeMs)
                .containsExactly(tuple("resolved", EXPIRES_MS));
          }
        });
  }

  @Test
  void zeroCountMinuteThenExpiryCarriesOnlyFiniteNumbers() throws Exception {
    try (var h = harness()) {
      // A region baseline for minute 0, so the seasonal error-rate alert fires as well and the
      // zero-count minute is read against a baseline (its sampling spread is infinite).
      h.processBroadcastElement(
          new PopulationBaseline(
              "eu", HourOfWeek.bucket(0), HourOfWeek.week(0), 0.01, 0.002, 100, 4, 500),
          0);
      history(h, 0, 3 * MINUTE_MS, 100, 10, 100.0);
      minute(h, KEY, 3 * MINUTE_MS, 0, 0, 100.0);

      h.watermark(EXPIRES_MS);

      List<AlertEvent> out = h.extractOutputValues();
      assertThat(out)
          .extracting(AlertEvent::detector, AlertEvent::state, AlertEvent::reason)
          .containsExactlyInAnyOrder(
              tuple("global_static", "firing", null),
              tuple("seasonal", "firing", null),
              tuple("global_static", "resolved", "expired"),
              tuple("seasonal", "resolved", "expired"));
      assertFinite(out);
    }
  }
}
