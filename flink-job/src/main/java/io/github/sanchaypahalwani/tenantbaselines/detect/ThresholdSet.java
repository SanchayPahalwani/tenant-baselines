package io.github.sanchaypahalwani.tenantbaselines.detect;

import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import java.util.Map;

/** Fixed levels valid for a time range: global levels, with optional per-key overrides. */
public record ThresholdSet(
    String detector,
    String setting,
    long validFromMs,
    long validToMs,
    Map<Signal, Level> global,
    Map<String, Map<Signal, Level>> perKey)
    implements java.io.Serializable {

  public boolean activeAt(long minuteMs) {
    return minuteMs >= validFromMs && minuteMs < validToMs;
  }

  /** The level for a key and signal: per-key first, then global, else null. */
  public Level levelFor(String key, Signal s) {
    Map<Signal, Level> own = perKey.get(key);
    if (own != null && own.containsKey(s)) {
      return own.get(s);
    }
    return global.get(s);
  }
}
