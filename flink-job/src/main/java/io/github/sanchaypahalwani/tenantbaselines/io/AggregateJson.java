package io.github.sanchaypahalwani.tenantbaselines.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.sanchaypahalwani.tenantbaselines.model.Keys;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Parses a pre-built minute aggregate, in the field names of the job's own aggregates output:
 * {@code {key, tenant, region, unit, minute_ms, count, errors, p95_latency_ms}}. Other fields, such
 * as that output's {@code run_id}, are ignored.
 */
public final class AggregateJson {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final long MINUTE_MS = 60_000L;

  /** How much of a bad row its error message quotes. */
  private static final int QUOTED_CHARS = 200;

  private AggregateJson() {}

  /**
   * Returns the row as a {@link MinuteAgg} of {@code runId}.
   *
   * @throws IllegalArgumentException naming the fault and quoting the row's first 200 characters:
   *     not JSON or not an object; a field missing or of the wrong type; {@code minute_ms} not a
   *     whole minute; {@code count} negative; {@code errors} outside 0..count; {@code key} not
   *     {@code tenant|region|unit}, or a '|' in one of those; {@code p95_latency_ms} null with
   *     requests, present without, or not finite
   */
  public static MinuteAgg parse(byte[] json, String runId) {
    JsonNode n;
    try {
      n = MAPPER.readTree(json);
    } catch (IOException e) {
      throw bad("not JSON", json);
    }
    if (n == null || !n.isObject()) {
      throw bad("not an object", json);
    }
    String key = text(n, "key", json);
    String tenant = text(n, "tenant", json);
    String region = text(n, "region", json);
    String unit = text(n, "unit", json);
    long minuteMs = integer(n, "minute_ms", json);
    long count = integer(n, "count", json);
    long errors = integer(n, "errors", json);
    JsonNode p95 = field(n, "p95_latency_ms", json);
    if (!p95.isNull() && !p95.isNumber()) {
      throw bad("p95_latency_ms is not a number", json);
    }

    if (Math.floorMod(minuteMs, MINUTE_MS) != 0) {
      throw bad("minute_ms is not a whole minute", json);
    }
    if (count < 0) {
      throw bad("count is negative", json);
    }
    if (errors < 0 || errors > count) {
      throw bad("errors is outside 0..count", json);
    }
    // A '|' in a part would make a key that matches, but that the job splits differently.
    if (tenant.indexOf('|') >= 0 || region.indexOf('|') >= 0 || unit.indexOf('|') >= 0) {
      throw bad("a '|' in tenant, region or unit", json);
    }
    if (!key.equals(Keys.of(tenant, region, unit))) {
      throw bad("key is not tenant|region|unit", json);
    }
    Double p95LatencyMs = null;
    if (p95.isNull()) {
      if (count > 0) {
        throw bad("p95_latency_ms is null with count > 0", json);
      }
    } else {
      if (count == 0) {
        throw bad("p95_latency_ms is present with count 0", json);
      }
      p95LatencyMs = p95.asDouble();
      if (!Double.isFinite(p95LatencyMs)) {
        throw bad("p95_latency_ms is not finite", json);
      }
    }
    return new MinuteAgg(runId, key, tenant, region, unit, minuteMs, count, errors, p95LatencyMs);
  }

  private static JsonNode field(JsonNode n, String name, byte[] json) {
    JsonNode v = n.get(name);
    if (v == null) {
      throw bad(name + " is missing", json);
    }
    return v;
  }

  private static String text(JsonNode n, String name, byte[] json) {
    JsonNode v = field(n, name, json);
    if (!v.isTextual()) {
      throw bad(name + " is not a string", json);
    }
    return v.asText();
  }

  private static long integer(JsonNode n, String name, byte[] json) {
    JsonNode v = field(n, name, json);
    if (!v.isIntegralNumber() || !v.canConvertToLong()) {
      throw bad(name + " is not an integer", json);
    }
    return v.asLong();
  }

  private static IllegalArgumentException bad(String fault, byte[] json) {
    String row = new String(json, StandardCharsets.UTF_8);
    String quoted = row.length() > QUOTED_CHARS ? row.substring(0, QUOTED_CHARS) : row;
    return new IllegalArgumentException("Bad aggregate row, " + fault + ": " + quoted);
  }
}
