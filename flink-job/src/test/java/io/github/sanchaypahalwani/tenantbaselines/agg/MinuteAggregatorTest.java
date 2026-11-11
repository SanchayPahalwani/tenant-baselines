package io.github.sanchaypahalwani.tenantbaselines.agg;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class MinuteAggregatorTest {

  private static final long MON = 1_767_571_200_000L;

  private static Event ev(long ts, double latency, boolean ok) {
    return new Event(ts, "a", "eu", "u1", latency, ok);
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, Event, MinuteAgg> harness(
      long replayEndMs, Duration expiry) throws Exception {
    return ProcessFunctionTestHarnesses.forKeyedProcessFunction(
        new MinuteAggregator("t", replayEndMs, expiry), Event::key, Types.STRING);
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, Event, MinuteAgg> harness()
      throws Exception {
    return harness(Long.MIN_VALUE, Duration.ofDays(7));
  }

  @Test
  void aggregatesOneMinute() throws Exception {
    try (var h = harness()) {
      h.processElement(ev(0, 100, true), 0);
      h.processElement(ev(10_000, 200, true), 10_000);
      h.processElement(ev(20_000, 300, false), 20_000);
      h.processWatermark(59_999);
      List<MinuteAgg> out = h.extractOutputValues();
      assertThat(out).hasSize(1);
      MinuteAgg m = out.get(0);
      assertThat(m.minuteMs()).isEqualTo(0);
      assertThat(m.count()).isEqualTo(3);
      assertThat(m.errors()).isEqualTo(1);
      assertThat(m.p95LatencyMs())
          .isEqualTo(LatencyHistogram.representative(LatencyHistogram.bucketOf(300)));
    }
  }

  @Test
  void emitsZeroMinutesWhenKeyGoesSilent() throws Exception {
    try (var h = harness()) {
      h.processElement(ev(0, 100, true), 0);
      h.processWatermark(179_999);
      List<MinuteAgg> out = h.extractOutputValues();
      assertThat(out).extracting(MinuteAgg::minuteMs).containsExactly(0L, 60_000L, 120_000L);
      assertThat(out.get(1).count()).isZero();
      assertThat(out.get(1).p95LatencyMs()).isNull();
      assertThat(out.get(2).count()).isZero();
      assertThat(out.get(2).p95LatencyMs()).isNull();
    }
  }

  @Test
  void lateEventGoesToSideOutput() throws Exception {
    try (var h = harness()) {
      h.processElement(ev(0, 100, true), 0);
      h.processWatermark(59_999);
      h.processElement(ev(30_000, 100, true), 30_000);
      assertThat(h.getSideOutput(MinuteAggregator.LATE)).hasSize(1);
      List<MinuteAgg> out = h.extractOutputValues();
      assertThat(out).hasSize(1);
      assertThat(out.get(0).count()).isEqualTo(1);
    }
  }

  @Test
  @Timeout(2)
  void stopsZeroFillAtFinalWatermark() throws Exception {
    try (var h = harness()) {
      h.processElement(ev(0, 100, true), 0);
      h.processWatermark(Long.MAX_VALUE);
      assertThat(h.extractOutputValues()).hasSize(1);
    }
  }

  @Test
  @Timeout(2)
  void zeroFillsUpToReplayEnd() throws Exception {
    try (var h = harness(300_000, Duration.ofDays(7))) {
      h.processElement(ev(0, 100, true), 0);
      h.processWatermark(Long.MAX_VALUE);
      List<MinuteAgg> out = h.extractOutputValues();
      assertThat(out).hasSize(5);
      assertThat(out.get(4).minuteMs()).isEqualTo(240_000);
    }
  }

  @Test
  void expiresAfterQuietPeriod() throws Exception {
    try (var h = harness(Long.MIN_VALUE, Duration.ofMinutes(10))) {
      h.processElement(ev(0, 100, true), 0);
      h.processWatermark(3_600_000);
      List<MinuteAgg> out = h.extractOutputValues();
      assertThat(out).hasSize(10);
      assertThat(out.get(9).minuteMs()).isEqualTo(540_000);

      h.processElement(ev(7_200_000, 100, true), 7_200_000);
      h.processWatermark(7_259_999);
      out = h.extractOutputValues();
      assertThat(out).hasSize(11);
      assertThat(out.get(10).count()).isEqualTo(1);
    }
  }

  @Test
  void lateEventIsNotAggregatedAfterALaterWatermark() throws Exception {
    try (var h = harness()) {
      h.processElement(ev(MON + 1_000, 100, true), MON + 1_000);
      h.processWatermark(MON + 120_000);
      h.processElement(ev(MON + 5_000, 100, true), MON + 5_000);
      h.processWatermark(MON + 240_000);

      List<MinuteAgg> rows =
          h.extractOutputValues().stream().filter(m -> m.minuteMs() == MON).toList();
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).count()).isEqualTo(1);
      assertThat(h.getSideOutput(MinuteAggregator.LATE)).hasSize(1);
    }
  }

  @Test
  void emittedMinuteCarriesTenantRegionUnit() throws Exception {
    try (var h = harness()) {
      h.processElement(ev(MON, 100, true), MON);
      h.processWatermark(MON + 119_999);
      List<MinuteAgg> out = h.extractOutputValues();
      assertThat(out).hasSize(2);
      assertThat(out.get(0).count()).isEqualTo(1);
      assertThat(out.get(1).count()).isZero();
      assertThat(out)
          .allSatisfy(
              m -> {
                assertThat(m.key()).isEqualTo("a|eu|u1");
                assertThat(m.tenant()).isEqualTo("a");
                assertThat(m.region()).isEqualTo("eu");
                assertThat(m.unit()).isEqualTo("u1");
              });
    }
  }

  @Test
  void onTimeOutOfOrderEventIsCounted() throws Exception {
    try (var h = harness()) {
      h.processElement(ev(MON + 30_000, 100, true), MON + 30_000);
      h.processElement(ev(MON + 10_000, 100, true), MON + 10_000);
      h.processWatermark(MON + 59_998);
      assertThat(h.extractOutputValues()).isEmpty();
      h.processWatermark(MON + 59_999);

      List<MinuteAgg> out = h.extractOutputValues();
      assertThat(out).hasSize(1);
      assertThat(out.get(0).count()).isEqualTo(2);
      assertThat(h.getSideOutput(MinuteAggregator.LATE)).isNull();
    }
  }
}
