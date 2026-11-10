package io.github.sanchaypahalwani.tenantbaselines.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.ScoreEvent;
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
}
