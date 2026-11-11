package io.github.sanchaypahalwani.tenantbaselines.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.ScoreEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.UnitStatusEvent;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonSerializationTest {

  @Test
  void writesSnakeCase() throws Exception {
    MinuteAgg agg = new MinuteAgg("live", "k", "t", "r", "u", 0, 0, 0, null);

    byte[] bytes = JsonSerialization.<MinuteAgg>json().serialize(agg);
    JsonNode node = new ObjectMapper().readTree(bytes);

    List<String> names = new ArrayList<>();
    node.fieldNames().forEachRemaining(names::add);
    assertThat(names)
        .containsExactlyInAnyOrder(
            "run_id", "key", "tenant", "region", "unit", "minute_ms", "count", "errors",
            "p95_latency_ms");
    assertThat(node.get("p95_latency_ms").isNull()).isTrue();
  }

  @Test
  void scoreEventWritesNulls() throws Exception {
    ScoreEvent score =
        new ScoreEvent("live", "t|r|u", "t", "r", "u", "error_rate", 60_000L, 0.25, null, null,
            null, null);

    byte[] bytes = JsonSerialization.<ScoreEvent>json().serialize(score);
    JsonNode node = new ObjectMapper().readTree(bytes);

    List<String> names = new ArrayList<>();
    node.fieldNames().forEachRemaining(names::add);
    assertThat(names)
        .containsExactlyInAnyOrder(
            "run_id", "key", "tenant", "region", "unit", "signal", "minute_ms", "value", "median",
            "spread", "score", "source");
    assertThat(node.get("minute_ms").asLong()).isEqualTo(60_000L);
    assertThat(node.get("value").asDouble()).isEqualTo(0.25);
    for (String absent : List.of("median", "spread", "score", "source")) {
      assertThat(node.get(absent).isNull()).as(absent).isTrue();
    }
  }

  @Test
  void keySchemasWriteUtf8Key() {
    MinuteAgg agg = new MinuteAgg("live", "t|r|u", "t", "r", "u", 0, 0, 0, null);
    ScoreEvent score =
        new ScoreEvent("live", "t|r|u", "t", "r", "u", "error_rate", 0, null, null, null, null,
            null);

    assertThat(new String(JsonSerialization.aggKey().serialize(agg), StandardCharsets.UTF_8))
        .isEqualTo("t|r|u");
    assertThat(new String(JsonSerialization.scoreKey().serialize(score), StandardCharsets.UTF_8))
        .isEqualTo("t|r|u");
  }

  @Test
  void unitStatusEventKeysAndKey() throws Exception {
    UnitStatusEvent event =
        new UnitStatusEvent("live", "u1", "eu", "drain", "error_rate", 60_000L, 70_000L,
            "dedicated");

    JsonNode node =
        new ObjectMapper().readTree(JsonSerialization.<UnitStatusEvent>json().serialize(event));

    List<String> names = new ArrayList<>();
    node.fieldNames().forEachRemaining(names::add);
    assertThat(names)
        .containsExactlyInAnyOrder(
            "run_id", "unit", "region", "status", "signal", "at_ms", "emitted_at_ms", "reason");
    assertThat(new String(JsonSerialization.unitKey().serialize(event), StandardCharsets.UTF_8))
        .isEqualTo("u1");
  }

  @Test
  void regionalAlertWritesEmptyTenantAndUnit() throws Exception {
    AlertEvent alert =
        new AlertEvent("id", "live", "seasonal", "k4", "region", "eu", "", "eu", "", "error_rate",
            "firing", 0L, 60_000L, 70_000L, 0.5, 0.1, null);

    JsonNode node =
        new ObjectMapper().readTree(JsonSerialization.<AlertEvent>json().serialize(alert));

    assertThat(node.get("tenant").isTextual()).isTrue();
    assertThat(node.get("tenant").asText()).isEmpty();
    assertThat(node.get("unit").isTextual()).isTrue();
    assertThat(node.get("unit").asText()).isEmpty();
  }
}
