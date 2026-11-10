package io.github.sanchaypahalwani.tenantbaselines.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.sanchaypahalwani.tenantbaselines.model.Event;

/** Parses the wire format {@code {ts, tn, rg, un, lat, ok}}. */
public final class EventJson {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private EventJson() {}

  /** Returns null for malformed JSON, missing fields or wrong types; never throws. */
  public static Event parse(byte[] json) {
    try {
      JsonNode n = MAPPER.readTree(json);
      if (n == null || !n.isObject()) {
        return null;
      }
      JsonNode ts = n.get("ts");
      JsonNode tn = n.get("tn");
      JsonNode rg = n.get("rg");
      JsonNode un = n.get("un");
      JsonNode lat = n.get("lat");
      JsonNode ok = n.get("ok");
      if (ts == null || !ts.isIntegralNumber() || !ts.canConvertToLong()
          || tn == null || !tn.isTextual()
          || rg == null || !rg.isTextual()
          || un == null || !un.isTextual()
          || lat == null || !lat.isNumber()
          || ok == null || !ok.isBoolean()) {
        return null;
      }
      return new Event(
          ts.asLong(), tn.asText(), rg.asText(), un.asText(), lat.asDouble(), ok.asBoolean());
    } catch (Exception e) {
      return null;
    }
  }
}
