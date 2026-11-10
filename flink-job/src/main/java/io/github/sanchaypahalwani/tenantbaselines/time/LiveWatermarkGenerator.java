package io.github.sanchaypahalwani.tenantbaselines.time;

import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import java.util.function.LongSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * Watermark generator for a live source. While the source is catching up on old data it follows
 * event time. Once event time is within the catch-up window of the wall clock it latches, and from
 * then on follows the wall clock, so minutes keep closing during a total outage when no events
 * arrive. It never emits a lower watermark.
 *
 * <p>An output that has seen no event by the end of the catch-up window is marked idle, so an
 * empty split does not hold back the others. It never emits the wall clock: Flink also runs a
 * generator for the source reader's main output, which never sees an event (the Kafka source
 * emits through per-split outputs) and whose watermarks are not combined with the splits'. A
 * wall-clock watermark from it would drop a backlog older than the wall clock as late.
 */
public class LiveWatermarkGenerator implements WatermarkGenerator<Event> {

    private final LongSupplier clockMs;
    private final long boundMs;
    private final long catchUpMs;
    private final long createdAt;

    private long maxTs = Long.MIN_VALUE;
    private boolean latched = false;
    private long lastEmitted = Long.MIN_VALUE;

    public LiveWatermarkGenerator(LongSupplier clockMs, long boundMs, long catchUpMs) {
        this.clockMs = clockMs;
        this.boundMs = boundMs;
        this.catchUpMs = catchUpMs;
        this.createdAt = clockMs.getAsLong();
    }

    @Override
    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        long now = clockMs.getAsLong();
        if (maxTs == Long.MIN_VALUE) {
            if (now - createdAt >= catchUpMs) {
                output.markIdle();
            }
            return;
        }
        if (!latched) {
            latched = maxTs >= now - catchUpMs;
        }
        long candidate = (latched ? Math.max(maxTs, now) : maxTs) - boundMs - 1;
        if (candidate > lastEmitted) {
            lastEmitted = candidate;
            output.emitWatermark(new Watermark(candidate));
        }
    }
}
