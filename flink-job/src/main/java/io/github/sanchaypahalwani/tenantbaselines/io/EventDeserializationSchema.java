package io.github.sanchaypahalwani.tenantbaselines.io;

import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;

/** Deserializes events; malformed payloads become null (skipped by the Kafka source) and are counted. */
public class EventDeserializationSchema implements DeserializationSchema<Event> {

  private transient Counter malformed;

  @Override
  public void open(InitializationContext context) {
    malformed = context.getMetricGroup().counter("malformedEvents");
  }

  @Override
  public Event deserialize(byte[] message) {
    Event event = EventJson.parse(message);
    if (event == null && malformed != null) {
      malformed.inc();
    }
    return event;
  }

  @Override
  public boolean isEndOfStream(Event nextElement) {
    return false;
  }

  @Override
  public TypeInformation<Event> getProducedType() {
    return TypeInformation.of(Event.class);
  }
}
