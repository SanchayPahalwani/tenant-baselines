package io.github.sanchaypahalwani.tenantbaselines.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.github.sanchaypahalwani.tenantbaselines.agg.MinuteAggregator;
import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TicksTest {

  private static final long MINUTE_MS = 60_000L;
  private static final String KEY = "a|eu|u1";

  /** 1970-01-05 00:00 UTC: decades behind the wall clock. */
  private static final long MON = 345_600_000L;

  @RegisterExtension
  static final MiniClusterExtension CLUSTER =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(2)
              .build());

  /** Parallelism 2, so the ticks cross a network channel and are serialized. */
  private static StreamExecutionEnvironment env() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    return env;
  }

  /** The start of the wall-clock minute three minutes ago. */
  private static long threeMinutesAgo() {
    long now = System.currentTimeMillis();
    return now - Math.floorMod(now, MINUTE_MS) - 3 * MINUTE_MS;
  }

  private static Event event(long ts) {
    return new Event(ts, "a", "eu", "u1", 1.0, true);
  }

  /** The first {@code n} minutes the aggregator closes on the events, given the ticks. */
  private static List<MinuteAgg> firstMinutes(
      DataStream<Event> events, DataStream<Event> ticks, int n) {
    DataStream<MinuteAgg> minutes =
        Ticks.withTicks(events, ticks)
            .keyBy(Event::key)
            .process(new MinuteAggregator("t", Long.MIN_VALUE, Duration.ofDays(7)));
    return assertTimeoutPreemptively(Duration.ofSeconds(60), () -> minutes.executeAndCollect(n));
  }

  /**
   * The events source is bounded: its end-of-input watermark, not idleness, hands over to the
   * ticks. The second minute has no event; only a tick's watermark can close it.
   */
  @Test
  void ticksCloseMinutesWhenTheEventsStop() {
    long m = threeMinutesAgo();
    StreamExecutionEnvironment env = env();
    DataStream<Event> events =
        env.fromData(event(m + 1_000), event(m + 2_000), event(m + 3_000))
            .assignTimestampsAndWatermarks(Watermarks.ticks(Duration.ofSeconds(1)));
    DataStream<Event> ticks = Ticks.source(env, 20, Duration.ofSeconds(1));

    assertThat(firstMinutes(events, ticks, 2))
        .extracting(MinuteAgg::key, MinuteAgg::minuteMs, MinuteAgg::count)
        .containsExactly(tuple(KEY, m, 3L), tuple(KEY, m + MINUTE_MS, 0L));
  }

  /**
   * The events source never finishes: one event at once, the next 100 s later. Only the source
   * going idle after 2 s lets the ticks close those minutes.
   */
  @Test
  void ticksCloseMinutesWhenAnOpenSourceGoesSilent() {
    long m = threeMinutesAgo();
    StreamExecutionEnvironment env = env();
    DataStream<Event> events =
        env.fromSource(
                new DataGeneratorSource<>(
                    i -> event(m + 1_000),
                    Long.MAX_VALUE,
                    RateLimiterStrategy.perSecond(0.01),
                    TypeInformation.of(Event.class)),
                Watermarks.events(Duration.ofSeconds(1), Duration.ofSeconds(2)),
                "events")
            .setParallelism(1);
    DataStream<Event> ticks = Ticks.source(env, 20, Duration.ofSeconds(1));

    assertThat(firstMinutes(events, ticks, 2))
        .extracting(MinuteAgg::key, MinuteAgg::minuteMs, MinuteAgg::count)
        .containsExactly(tuple(KEY, m, 1L), tuple(KEY, m + MINUTE_MS, 0L));
  }

  /**
   * Three whole minutes of history, decades behind the ticks. If the ticks set the watermark,
   * every event would be late and no minute would close.
   */
  @Test
  void ticksDoNotOutrunABacklog() {
    List<Event> backlog = new ArrayList<>();
    for (int i = 0; i < 180; i++) {
      backlog.add(event(MON + i * 1_000L));
    }
    StreamExecutionEnvironment env = env();
    DataStream<Event> events =
        env.fromData(backlog)
            .assignTimestampsAndWatermarks(Watermarks.ticks(Duration.ofSeconds(20)));
    DataStream<Event> ticks = Ticks.source(env, 20, Duration.ofSeconds(20));

    assertThat(firstMinutes(events, ticks, 3))
        .extracting(MinuteAgg::minuteMs, MinuteAgg::count)
        .containsExactly(
            tuple(MON, 60L), tuple(MON + MINUTE_MS, 60L), tuple(MON + 2 * MINUTE_MS, 60L));
  }
}
