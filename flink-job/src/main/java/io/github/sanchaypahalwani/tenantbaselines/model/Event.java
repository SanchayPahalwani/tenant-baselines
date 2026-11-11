package io.github.sanchaypahalwani.tenantbaselines.model;

/** One request observation as read from the events topic, or a live watermark tick. */
public record Event(long ts, String tenant, String region, String unit, double latencyMs, boolean ok) {

  /**
   * A tick: only its timestamp counts, and it is dropped before aggregation. A parsed event
   * always has a tenant, so it is never a tick.
   */
  public static Event tick(long ts) {
    return new Event(ts, null, null, null, 0.0, true);
  }

  public boolean isTick() {
    return tenant == null;
  }

  public String key() {
    return Keys.of(tenant, region, unit);
  }
}
