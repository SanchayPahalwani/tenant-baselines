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
     * Wall-clock-following generator for live input. No idleness: idleness would freeze the
     * watermark during a total outage, which is when minutes must keep closing.
     */
    public static WatermarkStrategy<Event> live() {
        return WatermarkStrategy.<Event>forGenerator(
                        ctx -> new LiveWatermarkGenerator(System::currentTimeMillis, 20_000, 60_000))
                .withTimestampAssigner((e, t) -> e.ts());
    }
}
