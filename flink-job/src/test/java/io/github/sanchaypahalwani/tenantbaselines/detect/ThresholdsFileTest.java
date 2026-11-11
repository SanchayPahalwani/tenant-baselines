package io.github.sanchaypahalwani.tenantbaselines.detect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class ThresholdsFileTest {

  @Test
  void loadsDemoFile() throws Exception {
    try (InputStream in = getClass().getResourceAsStream("/thresholds-demo.json")) {
      List<ThresholdSet> sets = ThresholdsFile.load(in);
      assertThat(sets).hasSize(2);
      ThresholdSet global = sets.get(0);
      ThresholdSet perKey = sets.get(1);
      assertThat(global.detector()).isEqualTo("global_static");
      assertThat(perKey.detector()).isEqualTo("per_key_static");
      for (ThresholdSet s : sets) {
        assertThat(s.setting()).isEqualTo("p99.9");
        assertThat(s.validFromMs()).isZero();
        assertThat(s.validToMs()).isEqualTo(Long.MAX_VALUE);
        for (Signal signal : Signal.values()) {
          assertThat(s.levelFor("no-such-key", signal)).isNotNull();
        }
      }
      assertThat(global.perKey()).isEmpty();
      assertThat(perKey.perKey()).isNotEmpty();
      String demoKey = perKey.perKey().keySet().iterator().next();
      assertThat(perKey.levelFor(demoKey, Signal.ERROR_RATE))
          .isNotEqualTo(perKey.levelFor("no-such-key", Signal.ERROR_RATE));
    }
  }

  private static final String G_OK =
      "\"global\":{\"error_rate\":{\"fire\":0.05,\"resolve\":0.025}}";

  private static List<ThresholdSet> loadJson(String json) throws Exception {
    return ThresholdsFile.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
  }

  private static String set(String detector, String setting, String validity, String rest) {
    return "{\"detector\":\"" + detector + "\",\"setting\":\"" + setting + "\","
        + validity + "," + rest + "}";
  }

  private static String file(String... sets) {
    return "{\"sets\":[" + String.join(",", sets) + "]}";
  }

  private static void assertRejected(String json, String... messageParts) {
    assertThatThrownBy(() -> loadJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContainingAll(messageParts);
  }

  @Test
  void rejectsMissingFields() {
    String from = "\"valid_from_ms\":0";
    assertRejected(
        file("{\"setting\":\"p99\"," + from + "," + G_OK + "}"), "thresholds set #0: ");
    assertRejected(
        file("{\"detector\":\"global_static\"," + from + "," + G_OK + "}"),
        "thresholds set #0: ");
    assertRejected(
        file("{\"detector\":\"global_static\",\"setting\":\"p99\"," + G_OK + "}"),
        "thresholds set global_static/p99: ",
        "valid_from_ms");
    assertRejected(
        file(set("global_static", "p99", from, "\"global\":{\"error_rate\":{\"fire\":0.05}}")),
        "thresholds set global_static/p99: ",
        "resolve");
    assertRejected(
        file(set("global_static", "p99", from, "\"global\":{\"error_rate\":{\"resolve\":0.05}}")),
        "thresholds set global_static/p99: ",
        "fire");
  }

  @Test
  void rejectsSlashInDetector() {
    assertRejected(
        file(set("global/static", "p99", "\"valid_from_ms\":0", G_OK)),
        "thresholds set global/static/p99: ",
        "'/'");
  }

  @Test
  void rejectsSeasonalAsDetector() {
    assertRejected(
        file(set("seasonal", "k3", "\"valid_from_ms\":0", G_OK)),
        "thresholds set seasonal/k3: ",
        "seasonal");
  }

  @Test
  void rejectsUnknownSignal() {
    assertRejected(
        file(
            set(
                "global_static",
                "p99",
                "\"valid_from_ms\":0",
                "\"global\":{\"error_ratio\":{\"fire\":0.05,\"resolve\":0.025}}")),
        "thresholds set global_static/p99: ",
        "error_ratio");
    assertRejected(
        file(
            set(
                "per_key_static",
                "p99",
                "\"valid_from_ms\":0",
                "\"per_key\":{\"a|eu|u1\":{\"error_ratio\":{\"fire\":0.05,\"resolve\":0.025}}}")),
        "thresholds set per_key_static/p99: ",
        "error_ratio");
  }

  @Test
  void rejectsMalformedPerKey() {
    for (String key : new String[] {"a|eu", "a|eu|", "|eu|u1", "a|eu|u1|x"}) {
      assertRejected(
          file(
              set(
                  "per_key_static",
                  "p99",
                  "\"valid_from_ms\":0",
                  "\"per_key\":{\"" + key + "\":{\"error_rate\":{\"fire\":0.05,\"resolve\":0.025}}}")),
          "thresholds set per_key_static/p99: ",
          key);
    }
  }

  @Test
  void rejectsWrongOrder() {
    assertRejected(
        file(
            set(
                "global_static",
                "p99",
                "\"valid_from_ms\":0",
                "\"global\":{\"error_rate\":{\"fire\":0.05,\"resolve\":0.06}}")),
        "thresholds set global_static/p99: ",
        "error_rate");
    assertRejected(
        file(
            set(
                "global_static",
                "p99",
                "\"valid_from_ms\":0",
                "\"global\":{\"volume_5m\":{\"fire\":5,\"resolve\":0}}")),
        "thresholds set global_static/p99: ",
        "volume_5m");
  }

  @Test
  void rejectsNonFiniteLevel() {
    assertRejected(
        file(
            set(
                "global_static",
                "p99",
                "\"valid_from_ms\":0",
                "\"global\":{\"error_rate\":{\"fire\":\"NaN\",\"resolve\":0.025}}")),
        "thresholds set global_static/p99: ",
        "error_rate");
  }

  @Test
  void rejectsEmptyValidity() {
    assertRejected(
        file(set("global_static", "p99", "\"valid_from_ms\":100,\"valid_to_ms\":100", G_OK)),
        "thresholds set global_static/p99: ",
        "valid_to_ms");
  }

  @Test
  void rejectsOverlappingSets() {
    assertRejected(
        file(
            set("global_static", "p99", "\"valid_from_ms\":0,\"valid_to_ms\":100", G_OK),
            set("global_static", "p99", "\"valid_from_ms\":50,\"valid_to_ms\":200", G_OK)),
        "thresholds set global_static/p99: ",
        "overlap");
  }

  @Test
  void acceptsAdjacentSets() throws Exception {
    List<ThresholdSet> sets =
        loadJson(
            file(
                set("global_static", "p99", "\"valid_from_ms\":0,\"valid_to_ms\":100", G_OK),
                set("global_static", "p99", "\"valid_from_ms\":100,\"valid_to_ms\":200", G_OK)));
    assertThat(sets).hasSize(2);
  }

  @Test
  void perKeyOverridesAndValidityWindows() throws Exception {
    List<ThresholdSet> sets =
        loadJson(
            file(
                set(
                    "per_key_static",
                    "p99",
                    "\"valid_from_ms\":1000,\"valid_to_ms\":2000",
                    G_OK
                        + ",\"per_key\":{\"a|eu|u1\":{\"error_rate\":{\"fire\":0.2,\"resolve\":0.1}}}")));
    ThresholdSet s = sets.get(0);
    assertThat(s.levelFor("a|eu|u1", Signal.ERROR_RATE).fire()).isEqualTo(0.2);
    assertThat(s.levelFor("b|eu|u1", Signal.ERROR_RATE).fire()).isEqualTo(0.05);
    assertThat(s.levelFor("a|eu|u1", Signal.LATENCY_P95)).isEqualTo(s.global().get(Signal.LATENCY_P95));
    assertThat(s.activeAt(1000)).isTrue();
    assertThat(s.activeAt(2000)).isFalse();
  }
}
