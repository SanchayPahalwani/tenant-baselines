package io.github.sanchaypahalwani.tenantbaselines.baseline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assertions.withinPercentage;

import io.github.sanchaypahalwani.tenantbaselines.model.Keys;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;

@Timeout(value = 60, threadMode = ThreadMode.SEPARATE_THREAD)
class PopulationBaselinesTest {

  /** Monday 00:00 of week 1, the start of bucket 0. */
  private static final long H = 345_600_000L;

  private static final long MINUTE_MS = 60_000L;
  private static final String KEY = "a|eu|u1";

  private static KeyedOneInputStreamOperatorTestHarness<String, MinuteAgg, PopulationBaseline>
      harness() throws Exception {
    return ProcessFunctionTestHarnesses.forKeyedProcessFunction(
        new PopulationBaselines(), a -> PopulationBaselines.SINGLE_KEY, Types.STRING);
  }

  /** Sends the key's first {@code minutes} minutes from {@code hourStart}, one error each. */
  private static void feed(
      KeyedOneInputStreamOperatorTestHarness<String, MinuteAgg, PopulationBaseline> h,
      String key,
      long hourStart,
      int minutes,
      long count,
      double p95)
      throws Exception {
    String[] parts = Keys.split(key);
    for (int m = 0; m < minutes; m++) {
      long ms = hourStart + m * MINUTE_MS;
      h.processElement(
          new MinuteAgg("t", key, parts[0], parts[1], parts[2], ms, count, 1, p95), ms);
    }
  }

  private static void endHour(
      KeyedOneInputStreamOperatorTestHarness<String, MinuteAgg, PopulationBaseline> h,
      long hourStart)
      throws Exception {
    h.processWatermark(hourStart + HourOfWeek.HOUR_MS - 1);
  }

  /** Bucket 0 of weeks 1 to 5, with p95 100, 200, 300, 400 and 500. */
  private static void feedFiveWeeks(
      KeyedOneInputStreamOperatorTestHarness<String, MinuteAgg, PopulationBaseline> h)
      throws Exception {
    for (int week = 1; week <= 5; week++) {
      long hour = H + (week - 1) * HourOfWeek.WEEK_MS;
      feed(h, KEY, hour, 60, 100, 100.0 * week);
      endHour(h, hour);
    }
  }

  private static List<PopulationBaseline> publishedAfter(int minutes) throws Exception {
    try (var h = harness()) {
      feed(h, KEY, H, minutes, 100, 100);
      endHour(h, H);
      return h.extractOutputValues();
    }
  }

  private static List<String> storedHistograms(
      KeyedOneInputStreamOperatorTestHarness<String, MinuteAgg, PopulationBaseline> h)
      throws Exception {
    KeyedStateBackend<String> backend = h.getOperator().getKeyedStateBackend();
    backend.setCurrentKey(PopulationBaselines.SINGLE_KEY);
    MapState<String, PopHistogram> state =
        backend.getPartitionedState(
            VoidNamespace.INSTANCE,
            VoidNamespaceSerializer.INSTANCE,
            new MapStateDescriptor<>(
                "pop-histograms", Types.STRING, TypeInformation.of(PopHistogram.class)));
    List<String> keys = new ArrayList<>();
    state.keys().forEach(keys::add);
    return keys;
  }

  @Test
  void publishesRegionAndGlobalAtHourEnd() throws Exception {
    try (var h = harness()) {
      for (String key : List.of("a|eu|u1", "b|eu|u1", "c|us|u2")) {
        feed(h, key, H, 60, 100, 100);
      }
      h.processWatermark(H + 3_599_998);
      assertThat(h.extractOutputValues()).isEmpty();

      h.processWatermark(H + 3_599_999);
      List<PopulationBaseline> out = h.extractOutputValues();
      assertThat(out)
          .extracting(PopulationBaseline::scope, PopulationBaseline::n)
          .containsExactlyInAnyOrder(tuple("eu", 120L), tuple("us", 60L), tuple("*", 180L));
      for (PopulationBaseline b : out) {
        assertThat(b.bucket()).isZero();
        assertThat(b.week()).isEqualTo(2);
        assertThat(b.errorMedian()).isCloseTo(0.01, withinPercentage(3));
        assertThat(b.latencyMedian()).isCloseTo(100, withinPercentage(3));
        assertThat(b.errorScaledMad()).isZero();
        assertThat(b.latencyScaledMad()).isZero();
      }
    }
  }

  @Test
  void skipsMinutesBelowMinimumEvidence() throws Exception {
    try (var h = harness()) {
      feed(h, KEY, H, 60, 19, 100);
      endHour(h, H);
      assertThat(h.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void countOfTwentyIsScored() throws Exception {
    try (var h = harness()) {
      feed(h, KEY, H, 30, 20, 100);
      endHour(h, H);
      assertThat(h.extractOutputValues())
          .extracting(PopulationBaseline::scope)
          .containsExactlyInAnyOrder("eu", "*");
    }
    try (var h = harness()) {
      feed(h, KEY, H, 30, 19, 100);
      endHour(h, H);
      assertThat(h.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void nullP95IsSkipped() throws Exception {
    try (var h = harness()) {
      String[] parts = Keys.split(KEY);
      for (int m = 0; m < 30; m++) {
        long ms = H + m * MINUTE_MS;
        h.processElement(
            new MinuteAgg("t", KEY, parts[0], parts[1], parts[2], ms, 25, 1, null), ms);
      }
      endHour(h, H);
      assertThat(h.extractOutputValues()).isEmpty();
    }
  }

  @Test
  void needsThirtyValues() throws Exception {
    assertThat(publishedAfter(29)).isEmpty();
    assertThat(publishedAfter(30))
        .extracting(PopulationBaseline::scope, PopulationBaseline::n)
        .containsExactlyInAnyOrder(tuple("eu", 30L), tuple("*", 30L));
  }

  @Test
  void usesLastFourWeeks() throws Exception {
    try (var h = harness()) {
      feedFiveWeeks(h);
      List<PopulationBaseline> forWeek6 =
          h.extractOutputValues().stream().filter(b -> b.week() == 6).toList();
      assertThat(forWeek6)
          .extracting(PopulationBaseline::scope)
          .containsExactlyInAnyOrder("eu", "*");
      for (PopulationBaseline b : forWeek6) {
        assertThat(b.bucket()).isZero();
        assertThat(b.n()).isEqualTo(240);
        assertThat(b.latencyMedian()).isCloseTo(300, withinPercentage(3));
      }
    }
  }

  @Test
  void dropsTheWeekThatLeavesTheWindow() throws Exception {
    try (var h = harness()) {
      feedFiveWeeks(h);
      assertThat(storedHistograms(h))
          .containsExactlyInAnyOrder(
              "eu|0|2", "eu|0|3", "eu|0|4", "eu|0|5", "*|0|2", "*|0|3", "*|0|4", "*|0|5");
    }
  }

  @Test
  void finalWatermarkPublishesOnce() throws Exception {
    try (var h = harness()) {
      feed(h, KEY, H, 60, 100, 100);
      h.processWatermark(Long.MAX_VALUE);
      assertThat(h.extractOutputValues())
          .extracting(PopulationBaseline::scope, PopulationBaseline::week)
          .containsExactlyInAnyOrder(tuple("eu", 2L), tuple("*", 2L));
    }
  }

  @Test
  void ofPicksTheSignalsBaseline() {
    PopulationBaseline b = new PopulationBaseline("eu", 0, 1, 0.01, 0.002, 100, 4, 500);
    assertThat(b.of(Signal.ERROR_RATE)).isEqualTo(new Baseline(0.01, 0.002));
    assertThat(b.of(Signal.LATENCY_P95)).isEqualTo(new Baseline(100, 4));
    assertThat(b.of(Signal.VOLUME_5M)).isNull();
  }

  @Test
  void stateAndBroadcastTypesAreFlinkPojos() {
    assertThat(TypeInformation.of(PopHistogram.class)).isInstanceOf(PojoTypeInfo.class);
    assertThat(TypeInformation.of(PopulationBaseline.class)).isInstanceOf(PojoTypeInfo.class);
  }
}
