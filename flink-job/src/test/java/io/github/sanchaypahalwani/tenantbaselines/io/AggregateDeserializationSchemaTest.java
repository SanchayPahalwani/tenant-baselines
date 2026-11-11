package io.github.sanchaypahalwani.tenantbaselines.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class AggregateDeserializationSchemaTest {

  /** A row the source skipped would leave a hole in a bounded eval run, so a bad row throws. */
  @Test
  void stampsTheRunIdAndThrowsOnABadRow() {
    AggregateDeserializationSchema schema = new AggregateDeserializationSchema("r");
    byte[] row =
        ("{\"key\":\"a|eu|u1\",\"tenant\":\"a\",\"region\":\"eu\",\"unit\":\"u1\",\"minute_ms\":0,"
                + "\"count\":0,\"errors\":0,\"p95_latency_ms\":null}")
            .getBytes(StandardCharsets.UTF_8);

    assertThat(schema.deserialize(row))
        .isEqualTo(new MinuteAgg("r", "a|eu|u1", "a", "eu", "u1", 0, 0, 0, null));
    assertThatThrownBy(() -> schema.deserialize("{}".getBytes(StandardCharsets.UTF_8)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
