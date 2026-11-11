package io.github.sanchaypahalwani.tenantbaselines.detect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import org.junit.jupiter.api.Test;

class AlertsTest {

  private static AlertEvent build(double value, double reference) {
    return Alerts.of(
        "r", "seasonal", "k4", "tenant", "a|eu|u1", "a", "eu", "u1", "error_rate", "firing",
        60_000, 240_000, value, reference, null);
  }

  @Test
  void nonFiniteValueAndReferenceBecomeZero() {
    AlertEvent e = build(Double.NaN, Double.POSITIVE_INFINITY);
    assertEquals(0.0, e.value());
    assertEquals(0.0, e.reference());
    assertEquals(0.0, build(Double.NEGATIVE_INFINITY, Double.NaN).value());
    AlertEvent finite = build(0.25, 0.25);
    assertEquals(0.25, finite.value());
    assertEquals(0.25, finite.reference());
  }

  @Test
  void idIsTheDeterministicId() {
    assertEquals(
        AlertIds.of("seasonal", "k4", "tenant", "a|eu|u1", "error_rate", 60_000),
        build(0.1, 0.03).alertId());
  }

  @Test
  void everyFieldLandsInItsOwnAccessor() {
    long before = System.currentTimeMillis();
    AlertEvent e =
        Alerts.of(
            "run-1", "det", "set", "region", "key-1", "ten", "reg", "uni", "latency_p95",
            "absorbed", 111_000, 222_000, 3.5, 4.5, "why");
    long after = System.currentTimeMillis();

    assertEquals(AlertIds.of("det", "set", "region", "key-1", "latency_p95", 111_000), e.alertId());
    assertEquals("run-1", e.runId());
    assertEquals("det", e.detector());
    assertEquals("set", e.setting());
    assertEquals("region", e.scope());
    assertEquals("key-1", e.key());
    assertEquals("ten", e.tenant());
    assertEquals("reg", e.region());
    assertEquals("uni", e.unit());
    assertEquals("latency_p95", e.signal());
    assertEquals("absorbed", e.state());
    assertEquals(111_000, e.startsAtMs());
    assertEquals(222_000, e.eventTimeMs());
    assertEquals(3.5, e.value());
    assertEquals(4.5, e.reference());
    assertEquals("why", e.reason());
    assertTrue(e.emittedAtMs() >= before && e.emittedAtMs() <= after);
  }
}
