package io.github.sanchaypahalwani.tenantbaselines.time;

import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/**
 * Watermark strategies for replayed (bounded) and live input, for the live ticks, and for the
 * pre-built minute aggregates of aggregates mode.
 */
public final class Watermarks {

    /** Bounded out-of-orderness of the live events and of the ticks. */
    public static final Duration BOUND = Duration.ofSeconds(20);

    /** A live partition without an event for this long is idle: left out of the watermark. */
    public static final Duration LIVE_IDLENESS = Duration.ofSeconds(60);

    private Watermarks() {}

    /** Bounded out-of-orderness, with idleness and alignment, for replaying recorded data. */
    public static WatermarkStrategy<Event> replay() {
        return WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withIdleness(Duration.ofSeconds(30))
                .withWatermarkAlignment("events", Duration.ofMinutes(1), Duration.ofSeconds(1))
                .withTimestampAssigner((e, t) -> e.ts());
    }

    /**
     * Plain event time per Kafka partition, for live input. The live job unions {@link Ticks}
     * into this stream, so minutes keep closing once every partition is idle.
     */
    public static WatermarkStrategy<Event> live() {
        return events(BOUND, LIVE_IDLENESS);
    }

    /** Bounded out-of-orderness; a split without an event for {@code idleness} goes idle. */
    public static WatermarkStrategy<Event> events(Duration bound, Duration idleness) {
        return WatermarkStrategy.<Event>forBoundedOutOfOrderness(bound)
                .withIdleness(idleness)
                .withTimestampAssigner((e, t) -> e.ts());
    }

    /** Bounded out-of-orderness without idleness, for the tick source. */
    public static WatermarkStrategy<Event> ticks(Duration bound) {
        return WatermarkStrategy.<Event>forBoundedOutOfOrderness(bound)
                .withTimestampAssigner((e, t) -> e.ts());
    }

    /**
     * Pre-built aggregates, read in (minute, key) order from a single split. Each is stamped with
     * the last millisecond of its minute, the timestamp MinuteAggregator's timer gives it in the
     * events modes, so the grouper's timers land on the same minute ends in both. Without an
     * assigner a record would carry its Kafka timestamp, with no error.
     */
    public static WatermarkStrategy<MinuteAgg> aggregates() {
        return WatermarkStrategy.<MinuteAgg>forMonotonousTimestamps()
                .withTimestampAssigner((a, t) -> a.minuteMs() + 59_999);
    }
}
