package io.github.sanchaypahalwani.tenantbaselines.time;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.junit.jupiter.api.Test;

class WatermarksTest {

  private static final TimestampAssignerSupplier.Context CONTEXT = () -> null;

  private static long assigned(WatermarkStrategy<Event> strategy, Event e) {
    TimestampAssigner<Event> assigner = strategy.createTimestampAssigner(CONTEXT);
    return assigner.extractTimestamp(e, -1);
  }

  @Test
  void replayAndLiveAssignTheEventsTimestamp() {
    Event e = new Event(1234, "a", "eu", "u1", 100.0, true);

    assertThat(assigned(Watermarks.replay(), e)).isEqualTo(1234);
    assertThat(assigned(Watermarks.live(), e)).isEqualTo(1234);
  }
}
