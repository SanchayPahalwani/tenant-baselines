package io.github.sanchaypahalwani.tenantbaselines.detect;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
