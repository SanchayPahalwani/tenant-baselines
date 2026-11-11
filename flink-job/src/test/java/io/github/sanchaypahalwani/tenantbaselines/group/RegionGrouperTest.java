package io.github.sanchaypahalwani.tenantbaselines.group;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assertions.within;

import io.github.sanchaypahalwani.tenantbaselines.detect.AlertIds;
import io.github.sanchaypahalwani.tenantbaselines.detect.Alerts;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.Keys;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

class RegionGrouperTest {

  static final String REGION = "eu";
  static final String SEASONAL = "seasonal";
  static final String K4 = "k4";
  static final String K6 = "k6";
  static final String ERROR_RATE = "error_rate";
  static final String LATENCY = "latency_p95";

  /** 1970-01-05 00:00 UTC, a Monday. */
  private static final long MON = 345_600_000L;

  static long min(long n) {
    return n * 60_000L;
  }

  static KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> harness()
      throws Exception {
    return ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
        new RegionGrouper("r", SEASONAL, K4, Duration.ofDays(7)),
        AlertEvent::region,
        MinuteAgg::region,
        Types.STRING);
  }

  /**
   * A fresh grouper harness started from a snapshot, set up like {@link #harness()}'s. {@code
   * ProcessFunctionTestHarnesses} returns one that is already open, so this one is built by hand.
   */
  static KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> restored(
      OperatorSubtaskState snapshot) throws Exception {
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

  /** Minute n of the key tenant|eu|unit, with the record timestamp of the minute's end. */
  static void agg(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h,
      String tenant,
      String unit,
      long n,
      long count)
      throws Exception {
    MinuteAgg m =
        new MinuteAgg(
            "r",
            Keys.of(tenant, REGION, unit),
            tenant,
            REGION,
            unit,
            min(n),
            count,
            0,
            count == 0 ? null : 100.0);
    h.processElement2(m, min(n) + 59_999);
  }

  /** 1200 requests on u1 in minute n: each tenant is active for that minute and the next 59. */
  static void warmAt(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h,
      long n,
      String... tenants)
      throws Exception {
    for (String t : tenants) {
      agg(h, t, "u1", n, 1200);
    }
  }

  static void warm(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h,
      String... tenants)
      throws Exception {
    warmAt(h, 59, tenants);
  }

  /** A candidate decided at the end of minute n, which started two minutes before it. */
  static AlertEvent candidate(
      String detector,
      String setting,
      String tenant,
      String unit,
      String signal,
      String state,
      long n) {
    return Alerts.of(
        "r",
        detector,
        setting,
        Alerts.TENANT,
        Keys.of(tenant, REGION, unit),
        tenant,
        REGION,
        unit,
        signal,
        state,
        min(n - 2),
        min(n + 1),
        0.1,
        0.05,
        null);
  }

  static AlertEvent fire(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h,
      String detector,
      String setting,
      String tenant,
      String unit,
      String signal,
      long n)
      throws Exception {
    AlertEvent c = candidate(detector, setting, tenant, unit, signal, Alerts.FIRING, n);
    h.processElement1(c, min(n) + 59_999);
    return c;
  }

  static AlertEvent fire(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h,
      String tenant,
      String unit,
      String signal,
      long n)
      throws Exception {
    return fire(h, SEASONAL, K4, tenant, unit, signal, n);
  }

  static AlertEvent resolve(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h,
      String detector,
      String setting,
      String tenant,
      String unit,
      String signal,
      long n)
      throws Exception {
    AlertEvent c = candidate(detector, setting, tenant, unit, signal, Alerts.RESOLVED, n);
    h.processElement1(c, min(n) + 59_999);
    return c;
  }

  static AlertEvent resolve(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h,
      String tenant,
      String unit,
      String signal,
      long n)
      throws Exception {
    return resolve(h, SEASONAL, K4, tenant, unit, signal, n);
  }

  /** Moves both inputs to the end of minute n and returns what that released. */
  static List<AlertEvent> wm(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h, long n)
      throws Exception {
    h.processBothWatermarks(new Watermark(min(n) + 59_999));
    return released(h);
  }

  /** The events that came out since the last call; it clears the harness's output. */
  static List<AlertEvent> released(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h) {
    List<AlertEvent> out = h.extractOutputValues();
    h.getOutput().clear();
    return out;
  }

  static String regionalId(String setting, String signal, long startsAtMs) {
    return AlertIds.of(SEASONAL, setting, Alerts.REGION, REGION, signal, startsAtMs);
  }

  static List<AlertEvent> withScope(List<AlertEvent> events, String scope) {
    return events.stream().filter(e -> e.scope().equals(scope)).toList();
  }

  /**
   * Four active tenants; a fires at 60 and b at 61, both seasonal/k4 on error_rate. Returns what
   * wm(62) released: a regional alert that absorbs both.
   */
  static List<AlertEvent> twoOfFour(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h)
      throws Exception {
    warm(h, "a", "b", "c", "d");
    fire(h, "a", "u1", ERROR_RATE, 60);
    fire(h, "b", "u1", ERROR_RATE, 61);
    return wm(h, 62);
  }

  @Test
  void tenantAlertIsReleasedAfterTwoMinutes() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d");
      AlertEvent c = fire(h, "a", "u1", ERROR_RATE, 60);

      assertThat(wm(h, 61)).isEmpty();

      List<AlertEvent> out = wm(h, 62);
      assertThat(out).hasSize(1);
      AlertEvent e = out.get(0);
      assertThat(e.state()).isEqualTo("firing");
      assertThat(e.scope()).isEqualTo("tenant");
      assertThat(e.alertId()).isEqualTo(c.alertId());
      assertThat(e.startsAtMs()).isEqualTo(3_480_000L).isEqualTo(c.startsAtMs());
      assertThat(e.eventTimeMs()).isEqualTo(3_780_000L);
      assertThat(e)
          .extracting(
              AlertEvent::runId,
              AlertEvent::detector,
              AlertEvent::setting,
              AlertEvent::key,
              AlertEvent::tenant,
              AlertEvent::region,
              AlertEvent::unit,
              AlertEvent::signal,
              AlertEvent::value,
              AlertEvent::reference,
              AlertEvent::reason)
          .containsExactly(
              "r", "seasonal", "k4", "a|eu|u1", "a", "eu", "u1", "error_rate", 0.1, 0.05, null);
    }
  }

  @Test
  void halfOfTheActiveTenantsFormOneRegionalAlert() throws Exception {
    try (var h = harness()) {
      List<AlertEvent> out = twoOfFour(h);

      assertThat(out).hasSize(3);
      List<AlertEvent> regional = withScope(out, "region");
      assertThat(regional).hasSize(1);
      AlertEvent r = regional.get(0);
      String id = AlertIds.of("seasonal", "k4", "region", "eu", "error_rate", 3_720_000L);
      assertThat(r.alertId()).isEqualTo(id);
      assertThat(r)
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
              "firing",
              "eu",
              "",
              "",
              "eu",
              "error_rate",
              3_720_000L,
              3_780_000L,
              0.5,
              0.5,
              null);

      List<AlertEvent> absorbed = withScope(out, "tenant");
      assertThat(absorbed)
          .extracting(AlertEvent::alertId)
          .containsExactlyInAnyOrder(
              candidate(SEASONAL, K4, "a", "u1", ERROR_RATE, Alerts.FIRING, 60).alertId(),
              candidate(SEASONAL, K4, "b", "u1", ERROR_RATE, Alerts.FIRING, 61).alertId());
      assertThat(absorbed)
          .allSatisfy(
              e -> {
                assertThat(e.state()).isEqualTo("absorbed");
                assertThat(e.eventTimeMs()).isEqualTo(3_780_000L);
                assertThat(e.reason()).isEqualTo("regional:" + id);
              });

      // b's hold would have ended at minute 63: it never fires as a tenant alert.
      assertThat(wm(h, 64)).isEmpty();
    }
  }

  @Test
  void regionNeedsThreeActiveTenants() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b");
      fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "b", "u1", ERROR_RATE, 60);

      assertThat(wm(h, 62))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::tenant)
          .containsExactly(tuple("tenant", "firing", "a"), tuple("tenant", "firing", "b"));
    }
  }

  @Test
  void underHalfReleasesTenantAlerts() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d", "e");
      fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "b", "u1", ERROR_RATE, 60);

      assertThat(wm(h, 62))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::tenant)
          .containsExactly(tuple("tenant", "firing", "a"), tuple("tenant", "firing", "b"));
    }
  }

  @Test
  void inactiveTenantsDoNotCount() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c");
      agg(h, "x", "u1", 59, 600);
      agg(h, "y", "u1", 59, 600);
      agg(h, "z", "u1", 59, 600);
      fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "b", "u1", ERROR_RATE, 60);
      fire(h, "x", "u1", ERROR_RATE, 60);

      List<AlertEvent> out = wm(h, 62);
      List<AlertEvent> regional = withScope(out, "region");
      assertThat(regional).hasSize(1);
      assertThat(regional.get(0).state()).isEqualTo("firing");
      assertThat(regional.get(0).value()).isCloseTo(2.0 / 3.0, within(1e-4));
      assertThat(withScope(out, "tenant"))
          .extracting(AlertEvent::state, AlertEvent::tenant)
          .containsExactlyInAnyOrder(
              tuple("absorbed", "a"), tuple("absorbed", "b"), tuple("absorbed", "x"));
    }
  }

  @Test
  void tenantWithTwoKeysCountsOnce() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d");
      fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "a", "u2", ERROR_RATE, 60);

      // One tenant of four is 25 %.
      assertThat(wm(h, 62))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::key)
          .containsExactly(
              tuple("tenant", "firing", "a|eu|u1"), tuple("tenant", "firing", "a|eu|u2"));
    }
  }

  @Test
  void alertsAlreadySentStaySent() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d");
      fire(h, "a", "u1", ERROR_RATE, 60);
      assertThat(wm(h, 62))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::tenant)
          .containsExactly(tuple("tenant", "firing", "a"));

      fire(h, "b", "u1", ERROR_RATE, 65);
      fire(h, "c", "u1", ERROR_RATE, 65);
      List<AlertEvent> out = wm(h, 67);
      assertThat(out).hasSize(3);
      List<AlertEvent> regional = withScope(out, "region");
      assertThat(regional).extracting(AlertEvent::state).containsExactly("firing");
      assertThat(regional.get(0).value()).isEqualTo(0.75);
      assertThat(withScope(out, "tenant"))
          .extracting(AlertEvent::state, AlertEvent::tenant)
          .containsExactlyInAnyOrder(tuple("absorbed", "b"), tuple("absorbed", "c"));

      AlertEvent resolved = resolve(h, "a", "u1", ERROR_RATE, 75);
      assertThat(wm(h, 75)).containsExactly(resolved);
    }
  }

  @Test
  void candidateDuringARegionalAlertIsAbsorbedWithoutAHold() throws Exception {
    try (var h = harness()) {
      twoOfFour(h);
      AlertEvent c = fire(h, "d", "u1", ERROR_RATE, 66);

      assertThat(wm(h, 65)).isEmpty();

      List<AlertEvent> out = wm(h, 66);
      assertThat(out).hasSize(1);
      AlertEvent e = out.get(0);
      assertThat(e.state()).isEqualTo("absorbed");
      assertThat(e.scope()).isEqualTo("tenant");
      assertThat(e.alertId()).isEqualTo(c.alertId());
      assertThat(e.eventTimeMs()).isEqualTo(min(67));
      assertThat(e.reason()).isEqualTo("regional:" + regionalId(K4, ERROR_RATE, min(62)));
    }
  }

  @Test
  void settingsAndSignalsAreGroupedApart() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d");
      fire(h, SEASONAL, K4, "a", "u1", ERROR_RATE, 60);
      fire(h, SEASONAL, K4, "b", "u1", ERROR_RATE, 60);
      fire(h, SEASONAL, K6, "a", "u1", ERROR_RATE, 60);
      fire(h, SEASONAL, K4, "c", "u1", LATENCY, 60);

      assertThat(wm(h, 62))
          .extracting(
              AlertEvent::scope,
              AlertEvent::state,
              AlertEvent::setting,
              AlertEvent::signal,
              AlertEvent::tenant)
          .containsExactlyInAnyOrder(
              tuple("region", "firing", "k4", "error_rate", ""),
              tuple("tenant", "absorbed", "k4", "error_rate", "a"),
              tuple("tenant", "absorbed", "k4", "error_rate", "b"),
              tuple("tenant", "firing", "k6", "error_rate", "a"),
              tuple("tenant", "firing", "k4", "latency_p95", "c"));
    }
  }

  @Test
  void resolveForUnknownAlertPassesThrough() throws Exception {
    try (var h = harness()) {
      AlertEvent resolved = resolve(h, "a", "u1", ERROR_RATE, 70);

      assertThat(wm(h, 70)).containsExactly(resolved);
    }
  }

  @Test
  void absorbedAlertResolvesSilently() throws Exception {
    try (var h = harness()) {
      twoOfFour(h);
      resolve(h, "b", "u1", ERROR_RATE, 70);

      assertThat(wm(h, 70)).isEmpty();
    }
  }

  @Test
  void candidatesAheadOfTheWatermarkAreAppliedInTimeOrder() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d", "e");
      fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "b", "u1", ERROR_RATE, 64);

      assertThat(wm(h, 66))
          .extracting(
              AlertEvent::scope, AlertEvent::state, AlertEvent::tenant, AlertEvent::eventTimeMs)
          .containsExactly(
              tuple("tenant", "firing", "a", min(63)), tuple("tenant", "firing", "b", min(67)));
    }
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d");
      fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "b", "u1", ERROR_RATE, 64);

      // At minute 62 b's candidate is not there yet: 1 of 4. At minute 66 it is: 2 of 4.
      assertThat(wm(h, 66))
          .extracting(
              AlertEvent::scope, AlertEvent::state, AlertEvent::tenant, AlertEvent::eventTimeMs)
          .containsExactly(
              tuple("tenant", "firing", "a", min(63)),
              tuple("region", "firing", "", min(67)),
              tuple("tenant", "absorbed", "b", min(67)));
    }
  }

  /** c, a and b fire at 60 among seven active tenants, each for k4 and k6, in the given order. */
  private static List<AlertEvent> threeOfSevenArrivingAs(String... order) throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d", "e", "f", "g");
      for (String t : order) {
        fire(h, SEASONAL, K4, t, "u1", ERROR_RATE, 60);
        fire(h, SEASONAL, K6, t, "u1", ERROR_RATE, 60);
      }
      return wm(h, 62);
    }
  }

  private static List<String> summary(List<AlertEvent> events) {
    return events.stream()
        .map(e -> e.alertId() + " " + e.state() + " " + e.eventTimeMs())
        .toList();
  }

  @Test
  void eventsOfOneMinuteComeOutInAFixedOrder() throws Exception {
    List<AlertEvent> first = threeOfSevenArrivingAs("c", "a", "b");

    // Three of seven is under half: six tenant alerts.
    assertThat(first)
        .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::setting, AlertEvent::tenant)
        .containsExactly(
            tuple("tenant", "firing", "k4", "a"),
            tuple("tenant", "firing", "k4", "b"),
            tuple("tenant", "firing", "k4", "c"),
            tuple("tenant", "firing", "k6", "a"),
            tuple("tenant", "firing", "k6", "b"),
            tuple("tenant", "firing", "k6", "c"));

    List<AlertEvent> second = threeOfSevenArrivingAs("b", "c", "a");
    assertThat(summary(second)).isEqualTo(summary(first));
  }

  /** One resolved and one firing candidate of the same alert family, in the given order. */
  private static List<String> resolvedAndFiringArrivingAs(boolean resolvedFirst) throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d");
      AlertEvent resolved =
          candidate(SEASONAL, K4, "a", "u1", ERROR_RATE, Alerts.RESOLVED, 60);
      AlertEvent firing = candidate(SEASONAL, K4, "a", "u1", ERROR_RATE, Alerts.FIRING, 61);
      assertThat(firing.startsAtMs()).isGreaterThan(resolved.startsAtMs());
      long ts = min(61) + 59_999;
      if (resolvedFirst) {
        h.processElement1(resolved, ts);
        h.processElement1(firing, ts);
      } else {
        h.processElement1(firing, ts);
        h.processElement1(resolved, ts);
      }
      List<String> out = new java.util.ArrayList<>(summary(wm(h, 61)));
      out.addAll(summary(wm(h, 62)));
      out.addAll(summary(wm(h, 63)));
      out.addAll(summary(wm(h, 64)));
      return out;
    }
  }

  @Test
  void candidatesOfOneMinuteAreSortedBeforeTheyApply() throws Exception {
    List<String> first = resolvedAndFiringArrivingAs(true);
    List<String> second = resolvedAndFiringArrivingAs(false);

    assertThat(second).isEqualTo(first);
    // Sorted, the firing candidate comes before the resolved one ("firing" < "resolved"), so the
    // resolve ends the held alert: nothing is released, not even at the hold's end (minute 63).
    // Applied in arrival order, resolved-first would pass the resolve and release a firing alert.
    assertThat(first).isEmpty();
  }

  @Test
  void lastActiveKeyLeavingEmptiesTheDenominator() throws Exception {
    // Five warm tenants; d and e are warm at minute 0 and x (d) goes silent so its hour sum
    // drops to 0 at minute 60. a and b fire at 60: their holds end at minute 62.
    try (var h = harness()) {
      warm(h, "a", "b", "c");
      warmAt(h, 0, "d", "e");
      agg(h, "d", "u1", 60, 0);
      fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "b", "u1", ERROR_RATE, 60);

      List<AlertEvent> out = wm(h, 62);
      assertThat(withScope(out, "region")).hasSize(1);
      assertThat(withScope(out, "region").get(0).value()).isEqualTo(0.5);
      assertThat(withScope(out, "tenant"))
          .extracting(AlertEvent::state, AlertEvent::tenant)
          .containsExactlyInAnyOrder(tuple("absorbed", "a"), tuple("absorbed", "b"));
    }
    try (var h = harness()) {
      warm(h, "a", "b");
      warmAt(h, 0, "c", "d", "e");
      agg(h, "c", "u1", 60, 0);
      agg(h, "d", "u1", 60, 0);
      agg(h, "e", "u1", 60, 0);
      fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "b", "u1", ERROR_RATE, 60);

      assertThat(wm(h, 62))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::tenant)
          .containsExactly(tuple("tenant", "firing", "a"), tuple("tenant", "firing", "b"));
    }
  }

  @Test
  void twoActiveKeysOfOneTenantCountOnceInTheDenominator() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d");
      agg(h, "d", "u2", 59, 1200);
      fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "b", "u1", ERROR_RATE, 60);

      List<AlertEvent> out = wm(h, 62);
      assertThat(withScope(out, "region")).hasSize(1);
      assertThat(withScope(out, "region").get(0).value()).isEqualTo(0.5);
    }
  }

  @Test
  void activityAheadOfTheWatermarkIsNotUsedEarly() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b");
      fire(h, "a", "u1", ERROR_RATE, 60);
      fire(h, "b", "u1", ERROR_RATE, 60);
      agg(h, "c", "u1", 63, 1200);

      // c becomes active at minute 63, after the holds ended at minute 62.
      assertThat(wm(h, 63))
          .extracting(
              AlertEvent::scope, AlertEvent::state, AlertEvent::tenant, AlertEvent::eventTimeMs)
          .containsExactly(
              tuple("tenant", "firing", "a", min(63)), tuple("tenant", "firing", "b", min(63)));
    }
  }

  /**
   * A candidate is applied at the end of the minute its record timestamp falls in. Both modes stamp
   * a minute's records with its last millisecond, so the candidate of minute MON is held from the
   * timer at MON + 59_999. A record stamped one millisecond later, as an assigner that took the
   * next minute's start would stamp it, is applied a minute later, and so is its release.
   */
  @Test
  void candidateStampedInsideItsMinuteAppliesAtThatMinutesEnd() throws Exception {
    long n = MON / 60_000L;
    try (var h = harness()) {
      h.processElement1(
          candidate(SEASONAL, K4, "a", "u1", ERROR_RATE, Alerts.FIRING, n), MON + 59_999);

      h.processBothWatermarks(new Watermark(MON + 59_998));
      assertThat(released(h)).isEmpty();
      h.processBothWatermarks(new Watermark(MON + 59_999 + RegionGrouper.HOLD_MS - 1));
      assertThat(released(h)).isEmpty();
      h.processBothWatermarks(new Watermark(MON + 59_999 + RegionGrouper.HOLD_MS));
      assertThat(released(h))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::eventTimeMs)
          .containsExactly(tuple("tenant", "firing", MON + 60_000 + RegionGrouper.HOLD_MS));
    }
    try (var h = harness()) {
      h.processElement1(
          candidate(SEASONAL, K4, "a", "u1", ERROR_RATE, Alerts.FIRING, n), MON + 60_000);

      h.processBothWatermarks(new Watermark(MON + 119_999 + RegionGrouper.HOLD_MS - 1));
      assertThat(released(h)).isEmpty();
      h.processBothWatermarks(new Watermark(MON + 119_999 + RegionGrouper.HOLD_MS));
      assertThat(released(h))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::eventTimeMs)
          .containsExactly(tuple("tenant", "firing", MON + 120_000 + RegionGrouper.HOLD_MS));
    }
  }

  @Test
  void stateTypesArePojos() {
    for (Class<?> c :
        List.of(
            KeyActivity.class,
            ActivityFlip.class,
            TenantAlert.class,
            RegionalAlert.class,
            AlertEvent.class,
            MinuteAgg.class)) {
      assertThat(TypeInformation.of(c)).as(c.getSimpleName()).isInstanceOf(PojoTypeInfo.class);
    }
  }
}
