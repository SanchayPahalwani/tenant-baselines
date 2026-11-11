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

  /**
   * Reads and validates the sets. Throws {@link IllegalArgumentException} with a message that
   * starts {@code thresholds set <detector>/<setting>: } (or {@code thresholds set #<index>: }
   * when a name is missing).
   */
  public static List<ThresholdSet> load(InputStream in) throws IOException {
    JsonNode root = MAPPER.readTree(in);
    List<ThresholdSet> sets = new ArrayList<>();
    int index = 0;
    for (JsonNode n : root.path("sets")) {
      sets.add(parseSet(n, index++));
    }
    checkOverlaps(sets);
    return sets;
  }

  private static ThresholdSet parseSet(JsonNode n, int index) {
    String detector = text(n.get("detector"));
    String setting = text(n.get("setting"));
    String name =
        detector == null || setting == null
            ? "#" + index
            : detector + "/" + setting;
    String prefix = "thresholds set " + name + ": ";
    if (detector == null) {
      throw new IllegalArgumentException(prefix + "missing detector");
    }
    if (setting == null) {
      throw new IllegalArgumentException(prefix + "missing setting");
    }
    if (detector.contains("/")) {
      throw new IllegalArgumentException(prefix + "detector must not contain '/'");
    }
    if (detector.equals("seasonal")) {
      throw new IllegalArgumentException(prefix + "detector must not be 'seasonal'");
    }
    JsonNode from = n.get("valid_from_ms");
    if (from == null || !from.isIntegralNumber()) {
      throw new IllegalArgumentException(prefix + "missing valid_from_ms");
    }
    JsonNode to = n.get("valid_to_ms");
    long validFrom = from.asLong();
    long validTo;
    if (to == null || to.isNull()) {
      validTo = Long.MAX_VALUE;
    } else if (to.isIntegralNumber()) {
      validTo = to.asLong();
    } else {
      throw new IllegalArgumentException(prefix + "valid_to_ms must be an integer or null");
    }
    if (validTo <= validFrom) {
      throw new IllegalArgumentException(
          prefix + "valid_to_ms " + validTo + " must be after valid_from_ms " + validFrom);
    }
    Map<Signal, Level> global = levels(n.path("global"), "global", prefix);
    Map<String, Map<Signal, Level>> perKey = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> keys = n.path("per_key").fields();
    while (keys.hasNext()) {
      Map.Entry<String, JsonNode> e = keys.next();
      String[] parts = e.getKey().split("\\|", -1);
      if (parts.length != 3 || parts[0].isEmpty() || parts[1].isEmpty() || parts[2].isEmpty()) {
        throw new IllegalArgumentException(
            prefix + "per_key key '" + e.getKey() + "' must be tenant|region|unit");
      }
      perKey.put(e.getKey(), levels(e.getValue(), "per_key " + e.getKey(), prefix));
    }
    return new ThresholdSet(detector, setting, validFrom, validTo, global, perKey);
  }

  private static String text(JsonNode node) {
    return node == null || !node.isTextual() || node.asText().isEmpty() ? null : node.asText();
  }

  private static Map<Signal, Level> levels(JsonNode node, String where, String prefix) {
    Iterator<String> names = node.fieldNames();
    while (names.hasNext()) {
      String name = names.next();
      boolean known = false;
      for (Signal s : Signal.values()) {
        known |= s.wire().equals(name);
      }
      if (!known) {
        throw new IllegalArgumentException(prefix + where + ": unknown signal '" + name + "'");
      }
    }
    Map<Signal, Level> out = new EnumMap<>(Signal.class);
    for (Signal s : Signal.values()) {
      JsonNode l = node.get(s.wire());
      if (l == null) {
        continue;
      }
      String at = where + " " + s.wire() + ": ";
      double fire = number(l.get("fire"), "fire", at, prefix);
      double resolve = number(l.get("resolve"), "resolve", at, prefix);
      boolean ordered = s.direction() == Signal.Direction.UP ? resolve <= fire : resolve >= fire;
      if (!ordered) {
        throw new IllegalArgumentException(
            prefix
                + at
                + (s.direction() == Signal.Direction.UP ? "resolve must be at most fire" : "resolve must be at least fire")
                + " (fire "
                + fire
                + ", resolve "
                + resolve
                + ")");
      }
      out.put(s, new Level(fire, resolve));
    }
    return out;
  }

  private static double number(JsonNode v, String field, String at, String prefix) {
    if (v == null || v.isNull()) {
      throw new IllegalArgumentException(prefix + at + "missing " + field);
    }
    if (!v.isNumber() || !Double.isFinite(v.asDouble())) {
      throw new IllegalArgumentException(prefix + at + field + " is not a finite number");
    }
    return v.asDouble();
  }

  private static void checkOverlaps(List<ThresholdSet> sets) {
    for (int i = 0; i < sets.size(); i++) {
      ThresholdSet b = sets.get(i);
      for (int j = 0; j < i; j++) {
        ThresholdSet a = sets.get(j);
        if (a.detector().equals(b.detector())
            && a.setting().equals(b.setting())
            && a.validFromMs() < b.validToMs()
            && b.validFromMs() < a.validToMs()) {
          throw new IllegalArgumentException(
              "thresholds set "
                  + b.detector()
                  + "/"
                  + b.setting()
                  + ": validity ["
                  + b.validFromMs()
                  + ", "
                  + b.validToMs()
                  + ") overlaps ["
                  + a.validFromMs()
                  + ", "
                  + a.validToMs()
                  + ")");
        }
      }
    }
  }
}
