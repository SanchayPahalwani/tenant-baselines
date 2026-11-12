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
      String demoKey = "t01|us-east|us-east-d-t01"; // the demo's dedicated unit of t01
      assertThat(perKey.perKey()).containsKey(demoKey);
      assertThat(perKey.levelFor(demoKey, Signal.ERROR_RATE))
          .isNotEqualTo(perKey.levelFor("no-such-key", Signal.ERROR_RATE));
    }
  }

  private static final long WEEK_4_MS = 1_769_385_600_000L;
  private static final long WEEK_5_MS = 1_769_990_400_000L;
  private static final long REPLAY_END_MS = 1_770_595_200_000L;

  /**
   * The file `eval_run.prepare` writes, here for dev seed 7 at 6:1 (synthetic): both static
   * detectors at four settings, fitted on weeks 1-3 for week 4 and on weeks 1-4 for week 5.
   */
  @Test
  void loadsAnEvalWorldFile() throws Exception {
    try (InputStream in = getClass().getResourceAsStream("/thresholds-eval-dev-s7-a6.json")) {
      List<ThresholdSet> sets = ThresholdsFile.load(in);
      assertThat(sets).hasSize(16);
      for (ThresholdSet s : sets) {
        assertThat(s.detector()).isIn("global_static", "per_key_static");
        assertThat(s.setting()).isIn("p99", "p99.9", "p99.97", "p99.99");
        assertThat(s.validFromMs()).isIn(WEEK_4_MS, WEEK_5_MS);
        long end = s.validFromMs() == WEEK_4_MS ? WEEK_5_MS : REPLAY_END_MS;
        assertThat(s.validToMs()).isEqualTo(end);
        assertThat(s.global()).containsOnlyKeys(Signal.values());
        if (s.detector().equals("global_static")) {
          assertThat(s.perKey()).isEmpty();
        } else {
          // t24 starts in week 4: no values of its own for the week-4 fit, a week of them later
          assertThat(s.perKey()).hasSize(s.validFromMs() == WEEK_4_MS ? 46 : 48);
        }
      }
      ThresholdSet week4 = find(sets, "per_key_static", "p99.9", WEEK_4_MS);
      ThresholdSet week5 = find(sets, "per_key_static", "p99.9", WEEK_5_MS);
      String newKey = "t24|ap-southeast|ap-southeast-s1";
      assertThat(week4.levelFor(newKey, Signal.ERROR_RATE))
          .isEqualTo(week4.global().get(Signal.ERROR_RATE));
      assertThat(week5.perKey().get(newKey)).containsOnlyKeys(Signal.values());
      String oldKey = "t01|us-east|us-east-d-t01";
      assertThat(week4.levelFor(oldKey, Signal.ERROR_RATE))
          .isNotEqualTo(week4.global().get(Signal.ERROR_RATE));
      for (ThresholdSet s : sets) {
        assertThat(s.activeAt(WEEK_4_MS)).isEqualTo(s.validFromMs() == WEEK_4_MS);
        assertThat(s.activeAt(WEEK_5_MS)).isEqualTo(s.validFromMs() == WEEK_5_MS);
      }
    }
  }

  private static ThresholdSet find(
      List<ThresholdSet> sets, String detector, String setting, long validFromMs) {
    return sets.stream()
        .filter(
            s ->
                s.detector().equals(detector)
                    && s.setting().equals(setting)
                    && s.validFromMs() == validFromMs)
        .findFirst()
        .orElseThrow();
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
