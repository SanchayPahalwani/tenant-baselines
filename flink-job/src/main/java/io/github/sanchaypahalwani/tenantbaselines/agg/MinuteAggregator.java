package io.github.sanchaypahalwani.tenantbaselines.agg;

import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import io.github.sanchaypahalwani.tenantbaselines.model.Keys;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.time.Duration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Builds one {@link MinuteAgg} per key per event-time minute. A minute closes when the watermark
 * passes its last millisecond. After the first event, minutes with no traffic are emitted with a
 * zero count until the key has been quiet for the expiry period. Events for minutes that are
 * already closed go to the {@link #LATE} side output.
 */
public class MinuteAggregator extends KeyedProcessFunction<String, Event, MinuteAgg> {

  public static final OutputTag<Event> LATE = new OutputTag<>("late-events") {};

  private static final long MINUTE_MS = 60_000L;
  private static final long LAST_MS_OF_MINUTE = MINUTE_MS - 1;

  private final String runId;
  private final long replayEndMs;
  private final Duration expiry;

  private transient MapState<Long, MinuteAccumulator> open;
  private transient ValueState<Long> lastEventMs;

  /**
   * @param replayEndMs end of the replayed data; zero-fill continues up to it when the final
   *     watermark arrives. {@code Long.MIN_VALUE} means there is none.
   */
  public MinuteAggregator(String runId, long replayEndMs, Duration expiry) {
    this.runId = runId;
    this.replayEndMs = replayEndMs;
    this.expiry = expiry;
  }

  @Override
  public void open(OpenContext openContext) {
    open =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "open-minutes", Types.LONG, TypeInformation.of(MinuteAccumulator.class)));
    lastEventMs =
        getRuntimeContext().getState(new ValueStateDescriptor<>("last-event-ms", Types.LONG));
  }

  @Override
  public void processElement(Event e, Context ctx, Collector<MinuteAgg> out) throws Exception {
    long minute = e.ts() - Math.floorMod(e.ts(), MINUTE_MS);
    long timer = minute + LAST_MS_OF_MINUTE;
    if (timer <= ctx.timerService().currentWatermark()) {
      ctx.output(LATE, e);
      return;
    }
    MinuteAccumulator acc = open.get(minute);
    if (acc == null) {
      acc = new MinuteAccumulator();
    }
    acc.count++;
    if (!e.ok()) {
      acc.errors++;
    }
    acc.buckets[LatencyHistogram.bucketOf(e.latencyMs())]++;
    open.put(minute, acc);
    ctx.timerService().registerEventTimeTimer(timer);
    Long last = lastEventMs.value();
    lastEventMs.update(last == null ? e.ts() : Math.max(last, e.ts()));
  }

  @Override
  public void onTimer(long t, OnTimerContext ctx, Collector<MinuteAgg> out) throws Exception {
    long minute = t - LAST_MS_OF_MINUTE;
    MinuteAccumulator acc = open.get(minute);
    String[] parts = Keys.split(ctx.getCurrentKey());
    long count = acc == null ? 0 : acc.count;
    long errors = acc == null ? 0 : acc.errors;
    Double p95 = acc == null ? null : LatencyHistogram.p95(acc.buckets, acc.count);
    out.collect(
        new MinuteAgg(
            runId, ctx.getCurrentKey(), parts[0], parts[1], parts[2], minute, count, errors, p95));
    open.remove(minute);

    Long last = lastEventMs.value();
    if (last == null || minute + MINUTE_MS - last >= expiry.toMillis()) {
      open.clear();
      lastEventMs.clear();
      return;
    }

    // A final watermark (Long.MAX_VALUE) would otherwise keep firing newly registered timers
    // forever, so past it we only fill up to the end of the replayed data.
    long next = t + MINUTE_MS;
    boolean finalWatermark = ctx.timerService().currentWatermark() == Long.MAX_VALUE;
    if (!finalWatermark || (replayEndMs != Long.MIN_VALUE && next <= replayEndMs - 1)) {
      ctx.timerService().registerEventTimeTimer(next);
    }
  }
}
