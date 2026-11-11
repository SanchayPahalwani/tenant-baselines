package io.github.sanchaypahalwani.tenantbaselines.time;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.junit.jupiter.api.Test;

class WatermarksTest {

  private static final TimestampAssignerSupplier.Context CONTEXT = () -> null;

  private static <T> long assigned(WatermarkStrategy<T> strategy, T element) {
    TimestampAssigner<T> assigner = strategy.createTimestampAssigner(CONTEXT);
    return assigner.extractTimestamp(element, -1);
  }

  @Test
  void replayAndLiveAssignTheEventsTimestamp() {
    Event e = new Event(1234, "a", "eu", "u1", 100.0, true);

    assertThat(assigned(Watermarks.replay(), e)).isEqualTo(1234);
    assertThat(assigned(Watermarks.live(), e)).isEqualTo(1234);
  }

  /** The timestamp MinuteAggregator's timer gives the aggregate in the events modes. */
  @Test
  void aggregatesAreStampedWithTheLastMillisecondOfTheirMinute() {
    MinuteAgg a = new MinuteAgg("r", "a|eu|u1", "a", "eu", "u1", 120_000, 40, 2, 81.5);

    assertThat(assigned(Watermarks.aggregates(), a)).isEqualTo(179_999);
  }
}
