package io.github.sanchaypahalwani.tenantbaselines.detect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Reads threshold sets from JSON. */
public final class ThresholdsFile {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ThresholdsFile() {}

  public static List<ThresholdSet> load(InputStream in) throws IOException {
    JsonNode root = MAPPER.readTree(in);
    List<ThresholdSet> sets = new ArrayList<>();
    for (JsonNode n : root.path("sets")) {
      JsonNode to = n.get("valid_to_ms");
      Map<String, Map<Signal, Level>> perKey = new HashMap<>();
      Iterator<Map.Entry<String, JsonNode>> keys = n.path("per_key").fields();
      while (keys.hasNext()) {
        Map.Entry<String, JsonNode> e = keys.next();
        perKey.put(e.getKey(), levels(e.getValue()));
      }
      sets.add(
          new ThresholdSet(
              n.get("detector").asText(),
              n.get("setting").asText(),
              n.get("valid_from_ms").asLong(),
              to == null || to.isNull() ? Long.MAX_VALUE : to.asLong(),
              levels(n.path("global")),
              perKey));
    }
    return sets;
  }

  private static Map<Signal, Level> levels(JsonNode node) {
    Map<Signal, Level> out = new EnumMap<>(Signal.class);
    for (Signal s : Signal.values()) {
      JsonNode l = node.get(s.wire());
      if (l != null) {
        out.put(s, new Level(l.get("fire").asDouble(), l.get("resolve").asDouble()));
      }
    }
    return out;
  }
}
