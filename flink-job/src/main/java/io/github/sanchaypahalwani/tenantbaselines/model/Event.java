package io.github.sanchaypahalwani.tenantbaselines.model;

/** One request observation as read from the events topic. */
public record Event(long ts, String tenant, String region, String unit, double latencyMs, boolean ok) {

  public String key() {
    return Keys.of(tenant, region, unit);
  }
}
