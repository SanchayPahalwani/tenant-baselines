package io.github.sanchaypahalwani.tenantbaselines.group;

import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;

/**
 * A tenant alert the grouper knows, from its firing candidate to its resolve. {@code status} is
 * {@link #HELD} until the hold ends at {@code heldUntilMs}, then {@link #SENT} or {@link
 * #ABSORBED}. {@code activeAtFire} says whether the tenant was active when the alert fired. This is
 * a Flink POJO: keep it public with public non-final fields.
 */
public class TenantAlert {

  public static final String HELD = "held";
  public static final String SENT = "sent";
  public static final String ABSORBED = "absorbed";

  public String status;
  public long heldUntilMs;
  public boolean activeAtFire;

  public String alertId;
  public String detector;
  public String setting;
  public String signal;
  public String key;
  public String tenant;
  public String unit;
  public String reason;
  public long startsAtMs;
  public double value;
  public double reference;

  public TenantAlert() {}

  /** The candidate's fields, with no status yet. */
  public static TenantAlert of(AlertEvent c) {
    TenantAlert a = new TenantAlert();
    a.alertId = c.alertId();
    a.detector = c.detector();
    a.setting = c.setting();
    a.signal = c.signal();
    a.key = c.key();
    a.tenant = c.tenant();
    a.unit = c.unit();
    a.reason = c.reason();
    a.startsAtMs = c.startsAtMs();
    a.value = c.value();
    a.reference = c.reference();
    return a;
  }
}
