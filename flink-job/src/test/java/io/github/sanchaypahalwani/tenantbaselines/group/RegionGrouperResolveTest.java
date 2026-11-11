package io.github.sanchaypahalwani.tenantbaselines.group;

import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.ERROR_RATE;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.K4;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.K6;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.SEASONAL;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.agg;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.candidate;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.fire;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.harness;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.min;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.regionalId;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.released;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.resolve;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.twoOfFour;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.warm;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.warmAt;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.withScope;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.wm;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assertions.within;

import io.github.sanchaypahalwani.tenantbaselines.detect.Alerts;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A regional alert's end, the fresh hold of what it absorbed, the final watermark, restore. */
class RegionGrouperResolveTest {

  private static final String VOLUME = "volume_5m";

  /**
   * Four active tenants; a and b fire at 60, both seasonal/k4 on error_rate. Returns what wm(62)
   * released: a regional alert that absorbs both.
   */
  private static List<AlertEvent> aAndBOfFour(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h)
      throws Exception {
    warm(h, "a", "b", "c", "d");
    fire(h, "a", "u1", ERROR_RATE, 60);
    fire(h, "b", "u1", ERROR_RATE, 60);
    List<AlertEvent> out = wm(h, 62);
    assertThat(out)
        .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::tenant)
        .containsExactly(
            tuple("region", "firing", ""),
            tuple("tenant", "absorbed", "a"),
            tuple("tenant", "absorbed", "b"));
    return out;
  }

  /** a's firing candidate of {@link #aAndBOfFour}, with its own id and start. */
  private static AlertEvent aAtSixty() {
    return candidate(SEASONAL, K4, "a", "u1", ERROR_RATE, Alerts.FIRING, 60);
  }

  @Test
  void regionalResolvesAfterFiveMinutesUnderAQuarter() throws Exception {
    try (var h = harness()) {
      List<AlertEvent> all = new ArrayList<>(aAndBOfFour(h));
      AlertEvent firing = withScope(all, "region").get(0);
      // 1 of 4 is 25 %, which is not under a quarter.
      resolve(h, "b", "u1", ERROR_RATE, 70);
      // 0 of 4 from minute 72 on.
      resolve(h, "a", "u1", ERROR_RATE, 72);

      // Minutes 72 to 75: four in a row under a quarter.
      assertThat(wm(h, 75)).isEmpty();

      List<AlertEvent> out = wm(h, 76);
      all.addAll(out);
      assertThat(out).hasSize(1);
      AlertEvent e = out.get(0);
      assertThat(e.alertId()).isEqualTo(firing.alertId());
      assertThat(e)
          .extracting(
              AlertEvent::runId,
              AlertEvent::detector,
              AlertEvent::setting,
              AlertEvent::scope,
              AlertEvent::state,
              AlertEvent::key,
              AlertEvent::tenant,
              AlertEvent::unit,
              AlertEvent::region,
              AlertEvent::signal,
              AlertEvent::startsAtMs,
              AlertEvent::eventTimeMs,
              AlertEvent::value,
              AlertEvent::reference,
              AlertEvent::reason)
          .containsExactly(
              "r",
              "seasonal",
              "k4",
              "region",
              "resolved",
              "eu",
              "",
              "",
              "eu",
              "error_rate",
              firing.startsAtMs(),
              4_620_000L,
              0.0,
              0.25,
              null);
      // The regional alert is over: no timer a minute any more.
      assertThat(h.numEventTimeTimers()).isZero();

      // a and b were absorbed, and never fired or resolved as tenant alerts.
      assertThat(withScope(all, "tenant")).extracting(AlertEvent::state).containsOnly("absorbed");
    }
  }

  @Test
  void shareBackAtAQuarterRestartsTheCount() throws Exception {
    try (var h = harness()) {
      aAndBOfFour(h);
      resolve(h, "b", "u1", ERROR_RATE, 70);
      resolve(h, "a", "u1", ERROR_RATE, 72);
      // Minutes 72 to 75: four in a row under a quarter.
      assertThat(wm(h, 75)).isEmpty();

      AlertEvent c = fire(h, "c", "u1", ERROR_RATE, 76);
      // 1 of 4 from minute 76: the count starts again.
      assertThat(wm(h, 80))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::alertId)
          .containsExactly(tuple("tenant", "absorbed", c.alertId()));

      resolve(h, "c", "u1", ERROR_RATE, 81);
      // Minutes 81 to 84: four in a row under a quarter.
      assertThat(wm(h, 84)).isEmpty();
      assertThat(wm(h, 85))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::eventTimeMs)
          .containsExactly(tuple("region", "resolved", min(86)));
    }
  }

  /**
   * After {@link #aAndBOfFour}, b resolves at 63 and five more tenants are active from minute 64,
   * so a's alert is 1 of 9 for minutes 64 to 68. Returns what wm(68) released, once wm(67) has
   * released nothing.
   */
  private static List<AlertEvent> aAloneOfNine(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h)
      throws Exception {
    aAndBOfFour(h);
    resolve(h, "b", "u1", ERROR_RATE, 63);
    warmAt(h, 64, "e", "f", "g", "h", "i");
    assertThat(wm(h, 67)).isEmpty();
    return wm(h, 68);
  }

  @Test
  void stillFiringAbsorbedAlertGetsAFreshHold() throws Exception {
    try (var h = harness()) {
      assertThat(aAloneOfNine(h))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::eventTimeMs)
          .containsExactly(tuple("region", "resolved", 4_140_000L));

      // a's alert still fires: it is held again, until minute 70.
      assertThat(wm(h, 69)).isEmpty();

      AlertEvent a = aAtSixty();
      assertThat(wm(h, 70))
          .extracting(
              AlertEvent::scope,
              AlertEvent::state,
              AlertEvent::alertId,
              AlertEvent::startsAtMs,
              AlertEvent::eventTimeMs)
          .containsExactly(tuple("tenant", "firing", a.alertId(), a.startsAtMs(), 4_260_000L));
    }
  }

  @Test
  void reheldAlertThatResolvesStaysSilent() throws Exception {
    try (var h = harness()) {
      assertThat(aAloneOfNine(h))
          .extracting(AlertEvent::scope, AlertEvent::state)
          .containsExactly(tuple("region", "resolved"));
      resolve(h, "a", "u1", ERROR_RATE, 69);

      assertThat(wm(h, 72)).isEmpty();
    }
  }

  @Test
  void reheldAlertCanJoinANewRegionalAlert() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d", "e", "f");
      AlertEvent a = fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "b", "u1", ERROR_RATE, 60);
      fire(h, "c", "u1", ERROR_RATE, 60);
      List<AlertEvent> all = new ArrayList<>(wm(h, 62));
      List<AlertEvent> first = withScope(all, "region");
      assertThat(first).extracting(AlertEvent::state).containsExactly("firing");
      AlertEvent r1 = first.get(0);
      assertThat(r1.value()).isEqualTo(0.5);
      assertThat(r1.startsAtMs()).isEqualTo(min(62));

      // 1 of 6 from minute 63.
      resolve(h, "b", "u1", ERROR_RATE, 63);
      resolve(h, "c", "u1", ERROR_RATE, 63);
      assertThat(wm(h, 66)).isEmpty();
      List<AlertEvent> resolved = wm(h, 67);
      all.addAll(resolved);
      assertThat(resolved)
          .extracting(
              AlertEvent::scope, AlertEvent::state, AlertEvent::alertId, AlertEvent::eventTimeMs)
          .containsExactly(tuple("region", "resolved", r1.alertId(), min(68)));

      // a is held again until minute 69; b and c fire again with new alerts.
      AlertEvent b = fire(h, "b", "u1", ERROR_RATE, 68);
      AlertEvent c = fire(h, "c", "u1", ERROR_RATE, 68);
      assertThat(List.of(b.startsAtMs(), c.startsAtMs())).containsOnly(min(66));

      List<AlertEvent> out = wm(h, 69);
      all.addAll(out);
      assertThat(out).hasSize(4);
      List<AlertEvent> second = withScope(out, "region");
      assertThat(second).hasSize(1);
      AlertEvent r2 = second.get(0);
      assertThat(r2.state()).isEqualTo("firing");
      assertThat(r2.startsAtMs()).isEqualTo(min(69));
      assertThat(r2.value()).isEqualTo(0.5);
      assertThat(r2.alertId())
          .isEqualTo(regionalId(K4, ERROR_RATE, min(69)))
          .isNotEqualTo(r1.alertId());
      assertThat(withScope(out, "tenant"))
          .extracting(AlertEvent::state, AlertEvent::alertId, AlertEvent::reason)
          .containsExactly(
              tuple("absorbed", a.alertId(), "regional:" + r2.alertId()),
              tuple("absorbed", b.alertId(), "regional:" + r2.alertId()),
              tuple("absorbed", c.alertId(), "regional:" + r2.alertId()));

      assertThat(withScope(all, "tenant")).extracting(AlertEvent::state).containsOnly("absorbed");
    }
  }

  @Test
  void outageDoesNotEmptyItsOwnDenominator() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c");
      fire(h, "a", "u1", VOLUME, 60);
      fire(h, "b", "u1", VOLUME, 60);
      fire(h, "c", "u1", VOLUME, 60);
      List<AlertEvent> formed = wm(h, 62);
      List<AlertEvent> regional = withScope(formed, "region");
      assertThat(regional).extracting(AlertEvent::state).containsExactly("firing");
      assertThat(regional.get(0).value()).isEqualTo(1.0);
      assertThat(withScope(formed, "tenant"))
          .extracting(AlertEvent::state, AlertEvent::tenant)
          .containsExactly(
              tuple("absorbed", "a"), tuple("absorbed", "b"), tuple("absorbed", "c"));

      // The traffic stops: each key is no longer active from minute 119, an hour after minute 59.
      for (long n = 60; n <= 130; n++) {
        for (String t : List.of("a", "b", "c")) {
          agg(h, t, "u1", n, 0);
        }
      }

      assertThat(wm(h, 130)).isEmpty();
    }
  }

  @Test
  @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void finalWatermarkReleasesHeldAlertsAndStops() throws Exception {
    try (var h = harness()) {
      aAndBOfFour(h);
      fire(h, SEASONAL, K6, "d", "u1", ERROR_RATE, 63);

      h.processBothWatermarks(new Watermark(Long.MAX_VALUE));

      assertThat(released(h))
          .extracting(
              AlertEvent::scope, AlertEvent::state, AlertEvent::setting, AlertEvent::tenant)
          .containsExactly(tuple("tenant", "firing", "k6", "d"));
      assertThat(h.numEventTimeTimers()).isZero();
    }
  }

  /**
   * After {@link #aAndBOfFour}, a and b resolve at 63, so the regional alert is under a quarter
   * from minute 63; d fires for seasonal/k6 at 80.
   */
  private static void regionalUnderAQuarterThenAnotherSetting(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h)
      throws Exception {
    aAndBOfFour(h);
    resolve(h, "a", "u1", ERROR_RATE, 63);
    resolve(h, "b", "u1", ERROR_RATE, 63);
    fire(h, SEASONAL, K6, "d", "u1", ERROR_RATE, 80);
  }

  @Test
  @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void finalWatermarkOutputDoesNotDependOnTheLastWatermark() throws Exception {
    List<AlertEvent> straight;
    try (var h = harness()) {
      regionalUnderAQuarterThenAnotherSetting(h);
      h.processBothWatermarks(new Watermark(Long.MAX_VALUE));
      straight = released(h);
      assertThat(h.numEventTimeTimers()).isZero();
    }
    List<AlertEvent> stepped = new ArrayList<>();
    try (var h = harness()) {
      regionalUnderAQuarterThenAnotherSetting(h);
      stepped.addAll(wm(h, 70));
      h.processBothWatermarks(new Watermark(Long.MAX_VALUE));
      stepped.addAll(released(h));
      assertThat(h.numEventTimeTimers()).isZero();
    }

    // Minutes 63 to 67 are under a quarter; d's hold ends at minute 82.
    assertThat(stepped)
        .extracting(
            AlertEvent::scope,
            AlertEvent::state,
            AlertEvent::setting,
            AlertEvent::tenant,
            AlertEvent::eventTimeMs)
        .containsExactly(
            tuple("region", "resolved", "k4", "", min(68)),
            tuple("tenant", "firing", "k6", "d", min(83)));
    assertThat(straight)
        .usingRecursiveFieldByFieldElementComparatorIgnoringFields("emittedAtMs")
        .containsExactlyElementsOf(stepped);
  }

  @Test
  void stateSurvivesSnapshotAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d");
      fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "b", "u1", ERROR_RATE, 61);
      // No watermark yet: the activity changes and both candidates are still buffered.
      snapshot = h.snapshot(1, 1);
    }
    List<AlertEvent> unbroken;
    try (var h = harness()) {
      unbroken = twoOfFour(h);
    }

    try (var h = restored(snapshot)) {
      // The timers of minutes 59, 60 and 61 came back with the snapshot.
      assertThat(h.numEventTimeTimers()).isEqualTo(3);

      List<AlertEvent> out = wm(h, 62);
      // The same three events as RegionGrouperTest.halfOfTheActiveTenantsFormOneRegionalAlert.
      assertThat(out)
          .extracting(AlertEvent::scope, AlertEvent::state)
          .containsExactly(
              tuple("region", "firing"), tuple("tenant", "absorbed"), tuple("tenant", "absorbed"));
      assertThat(out)
          .usingRecursiveFieldByFieldElementComparatorIgnoringFields("emittedAtMs")
          .containsExactlyElementsOf(unbroken);
    }
  }

  /** A fresh grouper harness started from a snapshot, set up like {@link RegionGrouperTest}'s. */
  private static KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent>
      restored(OperatorSubtaskState snapshot) throws Exception {
    KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h =
        new KeyedTwoInputStreamOperatorTestHarness<>(
            new KeyedCoProcessOperator<>(
                new RegionGrouper("r", SEASONAL, K4, Duration.ofDays(7))),
            AlertEvent::region,
            MinuteAgg::region,
            Types.STRING);
    h.initializeState(snapshot);
    h.open();
    return h;
  }

  @Test
  void streakSurvivesSnapshotAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (var h = harness()) {
      aAndBOfFour(h);
      resolve(h, "b", "u1", ERROR_RATE, 70);
      resolve(h, "a", "u1", ERROR_RATE, 72);
      // Minutes 72 to 74: three in a row under a quarter.
      assertThat(wm(h, 74)).isEmpty();
      snapshot = h.snapshot(1, 1);
    }

    try (var h = restored(snapshot)) {
      // The timer of minute 75 came back with the snapshot.
      assertThat(h.numEventTimeTimers()).isEqualTo(1);
      assertThat(wm(h, 75)).isEmpty();
      assertThat(wm(h, 76))
          .extracting(
              AlertEvent::scope,
              AlertEvent::state,
              AlertEvent::alertId,
              AlertEvent::startsAtMs,
              AlertEvent::eventTimeMs)
          .containsExactly(
              tuple("region", "resolved", regionalId(K4, ERROR_RATE, min(62)), min(62), min(77)));
    }
  }

  @Test
  void reheldAlertSurvivesSnapshotAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (var h = harness()) {
      assertThat(aAloneOfNine(h))
          .extracting(AlertEvent::scope, AlertEvent::state)
          .containsExactly(tuple("region", "resolved"));
      snapshot = h.snapshot(1, 1);
    }

    try (var h = restored(snapshot)) {
      // a's fresh hold ends at minute 70; its timer came back with the snapshot.
      assertThat(h.numEventTimeTimers()).isEqualTo(1);
      assertThat(wm(h, 69)).isEmpty();
      AlertEvent a = aAtSixty();
      assertThat(wm(h, 70))
          .extracting(
              AlertEvent::scope,
              AlertEvent::state,
              AlertEvent::alertId,
              AlertEvent::startsAtMs,
              AlertEvent::eventTimeMs)
          .containsExactly(tuple("tenant", "firing", a.alertId(), a.startsAtMs(), min(71)));
    }
  }

  @Test
  void holdEndingInTheMinuteAnotherCandidateArrives() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d");
      AlertEvent a = fire(h, "a", "u1", ERROR_RATE, 60);
      AlertEvent b = fire(h, "b", "u1", ERROR_RATE, 62);

      // b's candidate is applied in step 2 of the timer whose step 4 ends a's hold: 2 of 4.
      String id = regionalId(K4, ERROR_RATE, min(62));
      String reason = "regional:" + id;
      assertThat(wm(h, 62))
          .extracting(
              AlertEvent::scope,
              AlertEvent::state,
              AlertEvent::alertId,
              AlertEvent::startsAtMs,
              AlertEvent::eventTimeMs,
              AlertEvent::value,
              AlertEvent::reason)
          .containsExactly(
              tuple("region", "firing", id, 3_720_000L, 3_780_000L, 0.5, null),
              tuple("tenant", "absorbed", a.alertId(), 3_480_000L, 3_780_000L, 0.1, reason),
              tuple("tenant", "absorbed", b.alertId(), 3_600_000L, 3_780_000L, 0.1, reason));
    }
  }

  @Test
  void activityFlipInTheHoldEndMinuteCounts() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b");
      agg(h, "c", "u1", 62, 1200);
      AlertEvent a = fire(h, "a", "u1", ERROR_RATE, 60);
      AlertEvent b = fire(h, "b", "u1", ERROR_RATE, 60);

      // c becomes active in step 1 of the timer whose step 4 ends the holds: 2 of 3.
      List<AlertEvent> out = wm(h, 62);
      String id = regionalId(K4, ERROR_RATE, min(62));
      String reason = "regional:" + id;
      assertThat(out)
          .extracting(
              AlertEvent::scope,
              AlertEvent::state,
              AlertEvent::alertId,
              AlertEvent::startsAtMs,
              AlertEvent::eventTimeMs,
              AlertEvent::reason)
          .containsExactly(
              tuple("region", "firing", id, 3_720_000L, 3_780_000L, null),
              tuple("tenant", "absorbed", a.alertId(), 3_480_000L, 3_780_000L, reason),
              tuple("tenant", "absorbed", b.alertId(), 3_480_000L, 3_780_000L, reason));
      assertThat(out.get(0).value()).isCloseTo(2.0 / 3.0, within(1e-4));
    }
  }
}
