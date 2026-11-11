package io.github.sanchaypahalwani.tenantbaselines.time;

import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Wall-clock ticks for the live watermark. A union's watermark is the lowest of its inputs that
 * are not idle, so unioned into the events the ticks never run ahead of a partition that is not
 * idle, and they keep minutes closing once every partition is idle. They are dropped before
 * aggregation.
 */
public final class Ticks {

  private Ticks() {}

  /** One tick a second, with the live bound. */
  public static DataStream<Event> source(StreamExecutionEnvironment env) {
    return source(env, 1.0, Watermarks.BOUND);
  }

  /**
   * Ticks stamped with the wall clock when they are made, at parallelism 1. The source counts to
   * {@code Long.MAX_VALUE}, so it reports itself bounded but does not end.
   */
  public static DataStream<Event> source(
      StreamExecutionEnvironment env, double perSecond, Duration bound) {
    return env.fromSource(
            new DataGeneratorSource<>(
                i -> Event.tick(System.currentTimeMillis()),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(perSecond),
                TypeInformation.of(Event.class)),
            Watermarks.ticks(bound),
            "ticks")
        .setParallelism(1)
        .uid("ticks-source");
  }

  /** The events, carrying the lower of their own and the ticks' watermark. */
  public static DataStream<Event> withTicks(DataStream<Event> events, DataStream<Event> ticks) {
    return events.union(ticks).filter(e -> !e.isTick()).uid("drop-ticks").name("drop-ticks");
  }
}
