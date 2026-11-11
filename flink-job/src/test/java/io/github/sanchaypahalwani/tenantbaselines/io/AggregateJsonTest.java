package io.github.sanchaypahalwani.tenantbaselines.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class AggregateJsonTest {

  private static final String ROW =
      "{\"key\":\"a|eu|u1\",\"tenant\":\"a\",\"region\":\"eu\",\"unit\":\"u1\","
          + "\"minute_ms\":120000,\"count\":40,\"errors\":2,\"p95_latency_ms\":81.5}";

  private static MinuteAgg parse(String json) {
    return AggregateJson.parse(json.getBytes(StandardCharsets.UTF_8), "r");
  }

  /** The row fails to parse with the fault and the row's first 200 characters in the message. */
  private static void rejects(String json, String fault) {
    assertThatThrownBy(() -> parse(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(fault)
        .hasMessageContaining(json.substring(0, Math.min(200, json.length())));
  }

  @Test
  void parsesARow() {
    assertThat(parse(ROW))
        .isEqualTo(new MinuteAgg("r", "a|eu|u1", "a", "eu", "u1", 120_000, 40, 2, 81.5));

    String zero =
        ROW.replace(
            "\"count\":40,\"errors\":2,\"p95_latency_ms\":81.5",
            "\"count\":0,\"errors\":0,\"p95_latency_ms\":null");
    assertThat(parse(zero))
        .isEqualTo(new MinuteAgg("r", "a|eu|u1", "a", "eu", "u1", 120_000, 0, 0, null));
  }

  @Test
  void readsTheJobsOwnAggregatesOutputUnderTheGivenRunId() {
    MinuteAgg written = new MinuteAgg("events-run", "a|eu|u1", "a", "eu", "u1", 0, 7, 1, 64.0);
    byte[] json = JsonSerialization.<MinuteAgg>json().serialize(written);

    assertThat(AggregateJson.parse(json, "r"))
        .isEqualTo(new MinuteAgg("r", "a|eu|u1", "a", "eu", "u1", 0, 7, 1, 64.0));
  }

  @Test
  void rejectsBadRows() {
    rejects("not json", "not JSON");
    rejects("[1,2]", "not an object");
    rejects(ROW.replace(",\"unit\":\"u1\"", ""), "unit is missing");
    rejects(ROW.replace("\"tenant\":\"a\"", "\"tenant\":1"), "tenant is not a string");
    rejects(ROW.replace("\"count\":40", "\"count\":\"40\""), "count is not an integer");
    rejects(ROW.replace("\"count\":40", "\"count\":40.0"), "count is not an integer");
    rejects(ROW.replace("81.5", "\"81.5\""), "p95_latency_ms is not a number");
    rejects(ROW.replace(",\"p95_latency_ms\":81.5", ""), "p95_latency_ms is missing");
    rejects(ROW.replace("120000", "120001"), "minute_ms is not a whole minute");
    rejects(
        ROW.replace("\"count\":40,\"errors\":2", "\"count\":-1,\"errors\":0"), "count is negative");
    rejects(ROW.replace("\"errors\":2", "\"errors\":-1"), "errors is outside 0..count");
    rejects(ROW.replace("\"errors\":2", "\"errors\":41"), "errors is outside 0..count");
    rejects(
        ROW.replace("\"key\":\"a|eu|u1\"", "\"key\":\"a|eu|u2\""), "key is not tenant|region|unit");
    rejects(ROW.replace("81.5", "null"), "p95_latency_ms is null with count > 0");
    rejects(
        ROW.replace("\"count\":40,\"errors\":2", "\"count\":0,\"errors\":0"),
        "p95_latency_ms is present with count 0");
    rejects(ROW.replace("81.5", "1e400"), "p95_latency_ms is not finite");
    // The key would match, but the job could not split it back into its parts.
    rejects(
        ROW.replace(
            "\"key\":\"a|eu|u1\",\"tenant\":\"a\"", "\"key\":\"a|x|eu|u1\",\"tenant\":\"a|x\""),
        "'|' in tenant, region or unit");
  }

  @Test
  void quotesOnlyTheFirst200CharactersOfALongRow() {
    String unit = "u".repeat(300);
    String json = ROW.replace("\"unit\":\"u1\"", "\"unit\":\"" + unit + "\"");

    assertThatThrownBy(() -> parse(json))
        .hasMessageContaining("key is not tenant|region|unit")
        .hasMessageContaining(json.substring(0, 200))
        .hasMessageNotContaining(json.substring(0, 201));
  }
}
