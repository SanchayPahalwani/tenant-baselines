package io.github.sanchaypahalwani.tenantbaselines.group;

import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.ERROR_RATE;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.K6;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.LATENCY;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.SEASONAL;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.agg;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.fire;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.harness;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.min;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.released;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.resolve;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.restored;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.warm;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.warmAt;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.withScope;
import static io.github.sanchaypahalwani.tenantbaselines.group.RegionGrouperTest.wm;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.UnitStatusEvent;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit drain and undrain events: a unit drains on the live setting's sent error-rate or latency
 * alerts, never on volume and never while that setting has a regional alert.
 */
class UnitStatusTest {

  private static final String VOLUME = "volume_5m";

  /** Eight days in minutes: past the seven-day expiry of a unit's tenants. */
  private static final long D = 11_520;

  /** The unit status events since the last call; it clears them. */
  private static List<UnitStatusEvent> status(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h) {
    ConcurrentLinkedQueue<StreamRecord<UnitStatusEvent>> q =
        h.getSideOutput(RegionGrouper.UNIT_STATUS);
    if (q == null) {
      return List.of();
    }
    List<UnitStatusEvent> out = q.stream().map(StreamRecord::getValue).toList();
    q.clear();
    return out;
  }

  /** Every field but emittedAtMs, the wall clock. */
  private static Tuple row(UnitStatusEvent e) {
    return tuple(
        e.runId(), e.unit(), e.region(), e.status(), e.signal(), e.atMs(), e.reason());
  }

  /** a's only unit is u-d, and no other tenant is on it; 1200 requests make a active too. */
  private static void dedicatedUnitFor(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h,
      String tenant)
      throws Exception {
    agg(h, tenant, "u-d", 59, 1200);
  }

  /**
   * b, c and d on u1 and a on its dedicated unit u-d; a's error_rate alert fires at 60 and is sent
   * at minute 62 (one tenant of four), which drains u-d.
   */
  private static void aDrainsItsDedicatedUnit(
      KeyedTwoInputStreamOperatorTestHarness<String, AlertEvent, MinuteAgg, AlertEvent> h)
      throws Exception {
    warm(h, "b", "c", "d");
    dedicatedUnitFor(h, "a");
    fire(h, "a", "u-d", ERROR_RATE, 60);

    assertThat(wm(h, 61)).isEmpty();
    assertThat(status(h)).isEmpty();

    long before = System.currentTimeMillis();
    assertThat(wm(h, 62))
        .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::tenant)
        .containsExactly(tuple("tenant", "firing", "a"));
    long after = System.currentTimeMillis();
    List<UnitStatusEvent> drain = status(h);
    assertThat(drain)
        .extracting(UnitStatusTest::row)
        .containsExactly(tuple("r", "u-d", "eu", "drain", "error_rate", 3_780_000L, "dedicated"));
    assertThat(drain.get(0).emittedAtMs()).isBetween(before, after);
  }

  @Test
  void dedicatedUnitDrainsOnOneSentAlert() throws Exception {
    try (var h = harness()) {
      aDrainsItsDedicatedUnit(h);

      AlertEvent resolved = resolve(h, "a", "u-d", ERROR_RATE, 70);
      assertThat(wm(h, 70)).containsExactly(resolved);
      assertThat(status(h))
          .extracting(UnitStatusTest::row)
          .containsExactly(
              tuple("r", "u-d", "eu", "undrain", "error_rate", 4_260_000L, "cleared"));
    }
  }

  @Test
  void sharedUnitNeedsTwoTenantsOnTheSameSignal() throws Exception {
    try (var h = harness()) {
      warm(h, "a", "b", "c", "d", "e");

      fire(h, "a", "u1", ERROR_RATE, 60);
      assertThat(wm(h, 62))
          .extracting(AlertEvent::state, AlertEvent::tenant, AlertEvent::signal)
          .containsExactly(tuple("firing", "a", ERROR_RATE));
      assertThat(status(h)).isEmpty();

      fire(h, "b", "u1", LATENCY, 63);
      assertThat(wm(h, 65))
          .extracting(AlertEvent::state, AlertEvent::tenant, AlertEvent::signal)
          .containsExactly(tuple("firing", "b", LATENCY));
      assertThat(status(h)).isEmpty();

      // Two of five tenants on one signal is under half: c's alert is sent too.
      fire(h, "c", "u1", ERROR_RATE, 66);
      assertThat(wm(h, 68))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::tenant)
          .containsExactly(tuple("tenant", "firing", "c"));
      assertThat(status(h))
          .extracting(UnitStatusTest::row)
          .containsExactly(tuple("r", "u1", "eu", "drain", "error_rate", min(69), "shared"));
    }
  }

  @Test
  void volumeAlertsNeverDrain() throws Exception {
    try (var h = harness()) {
      dedicatedUnitFor(h, "a");
      warm(h, "b", "c", "d");
      fire(h, "a", "u-d", VOLUME, 60);

      assertThat(wm(h, 62))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::tenant, AlertEvent::signal)
          .containsExactly(tuple("tenant", "firing", "a", VOLUME));
      assertThat(status(h)).isEmpty();
    }
  }

  @Test
  void noDrainDuringARegionalAlert() throws Exception {
    try (var h = harness()) {
      // Four active tenants.
      aDrainsItsDedicatedUnit(h);

      // 3 of 4: a regional alert forms, and the unit is undrained in the same minute.
      fire(h, "b", "u1", ERROR_RATE, 65);
      fire(h, "c", "u1", ERROR_RATE, 65);
      assertThat(withScope(wm(h, 67), "region"))
          .extracting(AlertEvent::state, AlertEvent::startsAtMs)
          .containsExactly(tuple("firing", min(67)));
      assertThat(status(h))
          .extracting(UnitStatusTest::row)
          .containsExactly(tuple("r", "u-d", "eu", "undrain", "error_rate", min(68), "regional"));

      // 1 of 4 is not under a quarter: the regional alert stays, and a's sent alert drains nothing.
      resolve(h, "b", "u1", ERROR_RATE, 70);
      resolve(h, "c", "u1", ERROR_RATE, 70);
      assertThat(wm(h, 80)).isEmpty();
      assertThat(status(h)).isEmpty();

      // 1 of 8 from minute 81: the regional alert resolves after five minutes, at minute 85.
      warmAt(h, 81, "e", "f", "g", "h");
      assertThat(wm(h, 84)).isEmpty();
      assertThat(status(h)).isEmpty();
      assertThat(wm(h, 85))
          .extracting(AlertEvent::scope, AlertEvent::state, AlertEvent::eventTimeMs)
          .containsExactly(tuple("region", "resolved", min(86)));
      // The same timer drains u-d again: a's alert was sent and still fires.
      assertThat(status(h))
          .extracting(UnitStatusTest::row)
          .containsExactly(tuple("r", "u-d", "eu", "drain", "error_rate", min(86), "dedicated"));
    }
  }

  @Test
  void onlyTheLiveSettingDrains() throws Exception {
    try (var h = harness()) {
      dedicatedUnitFor(h, "a");
      warm(h, "b", "c", "d");
      fire(h, SEASONAL, K6, "a", "u-d", ERROR_RATE, 60);
      fire(h, "global_static", "default", "a", "u-d", ERROR_RATE, 60);

      assertThat(wm(h, 62))
          .extracting(
              AlertEvent::scope, AlertEvent::state, AlertEvent::detector, AlertEvent::setting)
          .containsExactlyInAnyOrder(
              tuple("tenant", "firing", SEASONAL, K6),
              tuple("tenant", "firing", "global_static", "default"));
      assertThat(status(h)).isEmpty();
    }
  }

  @Test
  void unitWithOneTenantInSevenDaysIsDedicated() throws Exception {
    try (var h = harness()) {
      agg(h, "a", "u2", 59, 1200);
      agg(h, "b", "u2", 59, 5);
      warm(h, "c", "d");
      fire(h, "a", "u2", ERROR_RATE, 60);

      // b sent u2 five requests in the last seven days: u2 has two tenants.
      assertThat(wm(h, 62))
          .extracting(AlertEvent::state, AlertEvent::tenant)
          .containsExactly(tuple("firing", "a"));
      assertThat(status(h)).isEmpty();
    }
    try (var h = harness()) {
      // b's requests are eight days older than the alert.
      agg(h, "b", "u2", 0, 5);
      agg(h, "a", "u2", D + 59, 1200);
      warmAt(h, D + 59, "c", "d");
      fire(h, "a", "u2", ERROR_RATE, D + 60);

      assertThat(wm(h, D + 62))
          .extracting(AlertEvent::state, AlertEvent::tenant)
          .containsExactly(tuple("firing", "a"));
      assertThat(status(h))
          .extracting(UnitStatusTest::row)
          .containsExactly(
              tuple("r", "u2", "eu", "drain", "error_rate", min(D + 63), "dedicated"));
    }
  }

  @Test
  void drainSurvivesSnapshotAndRestore() throws Exception {
    OperatorSubtaskState snapshot;
    try (var h = harness()) {
      aDrainsItsDedicatedUnit(h);
      snapshot = h.snapshot(1, 1);
    }

    try (var h = restored(snapshot)) {
      AlertEvent resolved = resolve(h, "a", "u-d", ERROR_RATE, 70);
      assertThat(wm(h, 70)).containsExactly(resolved);
      assertThat(status(h))
          .extracting(UnitStatusTest::row)
          .containsExactly(
              tuple("r", "u-d", "eu", "undrain", "error_rate", 4_260_000L, "cleared"));

      // u-d's one tenant came back with the snapshot: a new alert of a's drains it again.
      fire(h, "a", "u-d", ERROR_RATE, 72);
      assertThat(wm(h, 74))
          .extracting(AlertEvent::state, AlertEvent::tenant)
          .containsExactly(tuple("firing", "a"));
      assertThat(status(h))
          .extracting(UnitStatusTest::row)
          .containsExactly(tuple("r", "u-d", "eu", "drain", "error_rate", min(75), "dedicated"));
    }
  }

  @Test
  @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void finalWatermarkGivesEachStatusOnce() throws Exception {
    try (var h = harness()) {
      dedicatedUnitFor(h, "a");
      warm(h, "b", "c", "d");
      fire(h, "a", "u-d", ERROR_RATE, 60);
      // A regional alert of another setting, under a quarter from minute 63.
      fire(h, SEASONAL, K6, "b", "u1", ERROR_RATE, 60);
      fire(h, SEASONAL, K6, "c", "u1", ERROR_RATE, 60);
      resolve(h, SEASONAL, K6, "b", "u1", ERROR_RATE, 63);
      resolve(h, SEASONAL, K6, "c", "u1", ERROR_RATE, 63);
      resolve(h, "a", "u-d", ERROR_RATE, 70);

      h.processBothWatermarks(new Watermark(Long.MAX_VALUE));

      // The timers of minutes 64 to 67 counted the k6 alert's streak and repeated no status.
      assertThat(withScope(released(h), "region"))
          .extracting(AlertEvent::setting, AlertEvent::state, AlertEvent::eventTimeMs)
          .containsExactly(tuple(K6, "firing", min(63)), tuple(K6, "resolved", min(68)));
      assertThat(status(h))
          .extracting(UnitStatusTest::row)
          .containsExactly(
              tuple("r", "u-d", "eu", "drain", "error_rate", min(63), "dedicated"),
              tuple("r", "u-d", "eu", "undrain", "error_rate", min(71), "cleared"));
      assertThat(h.numEventTimeTimers()).isZero();
    }
  }

  @Test
  void unitStatusEventIsAPojo() {
    assertThat(TypeInformation.of(UnitStatusEvent.class)).isInstanceOf(PojoTypeInfo.class);
    assertThat(RegionGrouper.UNIT_STATUS.getTypeInfo()).isInstanceOf(PojoTypeInfo.class);
  }
}
