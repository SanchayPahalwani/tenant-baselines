package io.github.sanchaypahalwani.tenantbaselines.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.ScoreEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.UnitStatusEvent;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.SerializationSchema;

/** Serialization schemas for the job's Kafka outputs. */
public final class JsonSerialization {

  private JsonSerialization() {}

  /** Snake-case JSON; null fields are written as null. */
  public static <T> SerializationSchema<T> json() {
    return new JsonSchema<>();
  }

  public static SerializationSchema<MinuteAgg> aggKey() {
    return agg -> agg.key().getBytes(StandardCharsets.UTF_8);
  }

  public static SerializationSchema<ScoreEvent> scoreKey() {
    return score -> score.key().getBytes(StandardCharsets.UTF_8);
  }

  public static SerializationSchema<AlertEvent> alertKey() {
    return alert -> alert.key().getBytes(StandardCharsets.UTF_8);
  }

  /** The compacted unit-status topic rejects records without a key. */
  public static SerializationSchema<UnitStatusEvent> unitKey() {
    return status -> status.unit().getBytes(StandardCharsets.UTF_8);
  }

  private static final class JsonSchema<T> implements SerializationSchema<T> {

    private transient ObjectMapper mapper;

    @Override
    public byte[] serialize(T element) {
      if (mapper == null) {
        mapper = new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
      }
      try {
        return mapper.writeValueAsBytes(element);
      } catch (Exception e) {
        throw new IllegalStateException("Cannot serialize " + element, e);
      }
    }
  }
}
