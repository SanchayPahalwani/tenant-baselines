package io.github.sanchaypahalwani.tenantbaselines.model;

/**
 * A unit's drain or undrain, published only: nothing acts on it. {@code status} is {@link #DRAIN}
 * or {@link #UNDRAIN}; {@code signal} is the signal the unit is or was drained for; {@code atMs} is
 * the start of the minute after the one that decided it. {@code reason} is {@link #DEDICATED} or
 * {@link #SHARED} on a drain, {@link #CLEARED} or {@link #REGIONAL} on an undrain.
 */
public record UnitStatusEvent(
    String runId,
    String unit,
    String region,
    String status,
    String signal,
    long atMs,
    long emittedAtMs,
    String reason) {

  public static final String DRAIN = "drain";
  public static final String UNDRAIN = "undrain";

  /** One tenant sent the unit a request in the last seven days, and its alert was sent. */
  public static final String DEDICATED = "dedicated";

  /** Two or more tenants of the unit have a sent alert on the same signal. */
  public static final String SHARED = "shared";

  /** The alerts that drained the unit no longer do. */
  public static final String CLEARED = "cleared";

  /** A regional alert of the setting fires: no unit of the region stays drained. */
  public static final String REGIONAL = "regional";
}
