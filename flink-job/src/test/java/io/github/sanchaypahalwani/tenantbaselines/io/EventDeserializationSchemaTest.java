package io.github.sanchaypahalwani.tenantbaselines.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;

class EventDeserializationSchemaTest {

  @Test
  void countsMalformed() throws Exception {
    SimpleCounter counter = new SimpleCounter();
    MetricGroup group =
        new UnregisteredMetricsGroup() {
          @Override
          public Counter counter(String name) {
            return counter;
          }
        };
    EventDeserializationSchema schema = new EventDeserializationSchema();
    schema.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return group;
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
          }
        });

    assertThat(schema.deserialize("garbage".getBytes(StandardCharsets.UTF_8))).isNull();
    assertThat(schema.deserialize("{}".getBytes(StandardCharsets.UTF_8))).isNull();
    assertThat(counter.getCount()).isEqualTo(2);
  }
}
