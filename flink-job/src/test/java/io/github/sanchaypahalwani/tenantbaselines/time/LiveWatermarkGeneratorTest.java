package io.github.sanchaypahalwani.tenantbaselines.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.junit.jupiter.api.Test;

class LiveWatermarkGeneratorTest {

    static final class RecordingOutput implements WatermarkOutput {
        final List<Long> emitted = new ArrayList<>();
        int idleCalls = 0;

        @Override
        public void emitWatermark(Watermark watermark) {
            emitted.add(watermark.getTimestamp());
        }

        @Override
        public void markIdle() {
            idleCalls++;
        }

        @Override
        public void markActive() {}

        long last() {
            return emitted.get(emitted.size() - 1);
        }
    }

    private static final long NOW = 1_000_000_000_000L;

    private static Event at(long ts) {
        return new Event(ts, "t", "r", "u", 1.0, true);
    }

    @Test
    void followsEventTimeDuringBackfill() {
        AtomicLong clock = new AtomicLong(NOW);
        LiveWatermarkGenerator g = new LiveWatermarkGenerator(clock::get, 20_000, 60_000);
        RecordingOutput out = new RecordingOutput();
        g.onEvent(at(0), 0, out);
        g.onEvent(at(120_000), 120_000, out);
        g.onPeriodicEmit(out);
        assertEquals(99_999L, out.last());
    }

    @Test
    void keepsAdvancingWithWallClockWhenSilent() {
        AtomicLong clock = new AtomicLong(NOW);
        LiveWatermarkGenerator g = new LiveWatermarkGenerator(clock::get, 20_000, 60_000);
        RecordingOutput out = new RecordingOutput();
        g.onEvent(at(NOW - 5_000), NOW - 5_000, out);
        g.onPeriodicEmit(out);
        assertEquals(NOW - 20_001, out.last());
        clock.addAndGet(600_000);
        g.onPeriodicEmit(out);
        assertEquals(NOW + 600_000 - 20_001, out.last());
        assertEquals(0, out.idleCalls, "an output that has seen an event never goes idle");
    }

    @Test
    void outputWithoutEventsGoesIdleAfterCatchUpWindow() {
        AtomicLong clock = new AtomicLong(NOW);
        LiveWatermarkGenerator g = new LiveWatermarkGenerator(clock::get, 20_000, 60_000);
        RecordingOutput out = new RecordingOutput();
        clock.addAndGet(59_000);
        g.onPeriodicEmit(out);
        assertTrue(out.emitted.isEmpty());
        assertEquals(0, out.idleCalls);
        clock.addAndGet(2_000);
        g.onPeriodicEmit(out);
        assertEquals(1, out.idleCalls);
        clock.addAndGet(600_000);
        g.onPeriodicEmit(out);
        assertTrue(out.emitted.isEmpty(), "an output without events never emits the wall clock");
    }

    // Flink also runs a generator for the source reader's main output, which never sees an event
    // (the Kafka source emits through per-split outputs) and whose watermarks are not combined
    // with the splits'. A wall-clock watermark from it would drop a backlog older than the wall
    // clock as late.
    @Test
    void backlogReadAfterCatchUpWindowFollowsEventTime() {
        AtomicLong clock = new AtomicLong(NOW);
        LiveWatermarkGenerator g = new LiveWatermarkGenerator(clock::get, 20_000, 60_000);
        RecordingOutput out = new RecordingOutput();
        clock.addAndGet(120_000);
        g.onPeriodicEmit(out);
        int idleBefore = out.idleCalls;
        long weeksAgo = NOW - 21 * 86_400_000L;
        g.onEvent(at(weeksAgo), weeksAgo, out);
        g.onPeriodicEmit(out);
        assertEquals(List.of(weeksAgo - 20_001), out.emitted);
        assertEquals(idleBefore, out.idleCalls, "no markIdle() after the first event");
    }

    @Test
    void neverDecreases() {
        AtomicLong clock = new AtomicLong(NOW);
        LiveWatermarkGenerator g = new LiveWatermarkGenerator(clock::get, 20_000, 60_000);
        RecordingOutput out = new RecordingOutput();
        long ahead = NOW + 3_600_000;
        g.onEvent(at(ahead), ahead, out);
        g.onPeriodicEmit(out);
        int count = out.emitted.size();
        long first = out.last();
        clock.addAndGet(-1_000_000);
        g.onPeriodicEmit(out);
        assertEquals(count, out.emitted.size());
        assertEquals(first, out.last());
    }
}
