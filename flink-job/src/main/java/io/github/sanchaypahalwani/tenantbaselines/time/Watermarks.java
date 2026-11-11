package io.github.sanchaypahalwani.tenantbaselines.time;

import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/** Watermark strategies for replayed (bounded) and live input. */
public final class Watermarks {

    private Watermarks() {}

    /** Bounded out-of-orderness, with idleness and alignment, for replaying recorded data. */
    public static WatermarkStrategy<Event> replay() {
        return WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withIdleness(Duration.ofSeconds(30))
                .withWatermarkAlignment("events", Duration.ofMinutes(1), Duration.ofSeconds(1))
                .withTimestampAssigner((e, t) -> e.ts());
    }

    /**
     * Wall-clock-following generator for live input.
     *
     * <p>A split that has seen an event never goes idle. Once it is within the 60 s catch-up
     * window of the wall clock, it follows the wall clock, so minutes keep closing when every
     * producer goes silent.
     *
     * <p>An output that has never seen an event marks itself idle after the catch-up window
     * instead of emitting the wall clock. Flink 2.2.1 also runs a generator on the source
     * reader's main output, which never sees Kafka records. If it latched to the wall clock, it
     * would override the slowest split and drop the backlog as late.
     *
     * <p>Limitation: if the job restores, or a task fails over, while every partition is silent,
     * the watermark stays put until the first event arrives. The zero-traffic rule cannot page
     * for that outage.
     */
    public static WatermarkStrategy<Event> live() {
        return WatermarkStrategy.<Event>forGenerator(
                        ctx -> new LiveWatermarkGenerator(System::currentTimeMillis, 20_000, 60_000))
                .withTimestampAssigner((e, t) -> e.ts());
    }
}
