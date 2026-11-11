package io.github.sanchaypahalwani.tenantbaselines.io;

import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Deserializes pre-built minute aggregates under the job's run id. A malformed row throws, which
 * fails the job: a bounded eval run must not skip input silently.
 */
public class AggregateDeserializationSchema implements DeserializationSchema<MinuteAgg> {

  private final String runId;

  public AggregateDeserializationSchema(String runId) {
    this.runId = runId;
  }

  @Override
  public MinuteAgg deserialize(byte[] message) {
    return AggregateJson.parse(message, runId);
  }

  @Override
  public boolean isEndOfStream(MinuteAgg nextElement) {
    return false;
  }

  @Override
  public TypeInformation<MinuteAgg> getProducedType() {
    return TypeInformation.of(MinuteAgg.class);
  }
}
