package io.github.sanchaypahalwani.tenantbaselines;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.github.sanchaypahalwani.tenantbaselines.baseline.HourOfWeek;
import io.github.sanchaypahalwani.tenantbaselines.detect.ThresholdSet;
import io.github.sanchaypahalwani.tenantbaselines.detect.ThresholdsFile;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import io.github.sanchaypahalwani.tenantbaselines.model.Keys;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.ScoreEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.Signal;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PipelineTest {

  private static final long MINUTE_MS = 60_000L;
  private static final long REPLAY_END_MS = 15 * MINUTE_MS;
  private static final String KEY = Keys.of("acme", "eu", "checkout");

  /** 1970-01-05 00:00 UTC, a Monday: the start of hour-of-week bucket 0. */
  private static final long MON = 345_600_000L;

  private static final int SEASONAL_RPM = 25;

  /** The first error minute of the seasonal input, two weeks and 10 minutes after MON. */
  private static final long ERRORS_FROM_MS = MON + 2 * HourOfWeek.WEEK_MS + 10 * MINUTE_MS;

  private static final long SEASONAL_END_MS = ERRORS_FROM_MS + 5 * MINUTE_MS;

  @RegisterExtension
  static final MiniClusterExtension CLUSTER =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(2)
              .build());

  private static JobConfig config(long replayEndMs) {
    return JobConfig.parse(
        new String[] {
          "--run-id", "test", "--mode", "replay", "--replay-end-ms", "" + replayEndMs,
          "--scores", "true"
        });
  }

  private static List<ThresholdSet> thresholds() throws Exception {
    try (InputStream in = PipelineTest.class.getResourceAsStream("/thresholds-demo.json")) {
      return ThresholdsFile.load(in);
    }
  }

  /** 100 events a minute for minutes 0-14; from minute 10 every second event is an error. */
  private static List<Event> events() {
    List<Event> events = new ArrayList<>();
    for (int minute = 0; minute < 15; minute++) {
      for (int i = 0; i < 100; i++) {
        long ts = minute * MINUTE_MS + i * 600L;
        boolean ok = minute < 10 || i % 2 == 0;
        events.add(new Event(ts, "acme", "eu", "checkout", 100.0, ok));
      }
    }
    return events;
  }

  private static DataStream<Event> withTimestamps(DataStream<Event> events) {
    return events.assignTimestampsAndWatermarks(
        WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner((e, t) -> e.ts()));
  }

  private static Pipeline.Outputs build() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Event> events = withTimestamps(env.fromData(events()));
    return Pipeline.build(events, config(REPLAY_END_MS), thresholds());
  }

  /**
   * Event {@code i} of the seasonal input: {@link #SEASONAL_RPM} evenly spaced events a minute
   * from {@link #MON}, all ok until {@link #ERRORS_FROM_MS}, then every second one an error.
   */
  private static Event seasonalEvent(long i) {
    long ts = MON + i * (MINUTE_MS / SEASONAL_RPM);
    boolean ok = ts < ERRORS_FROM_MS || (i % SEASONAL_RPM) % 2 == 0;
    return new Event(ts, "acme", "eu", "checkout", 100.0, ok);
  }

  /**
   * The seasonal input, generated rather than held in a list: about half a million events. A
   * single source split keeps them in time order; each subtask after it still sees them in order.
   */
  private static Pipeline.Outputs buildSeasonal() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    long n = (SEASONAL_END_MS - MON) / MINUTE_MS * SEASONAL_RPM;
    DataStream<Event> events =
        env.fromSequence(0, n - 1).setParallelism(1).map(PipelineTest::seasonalEvent);
    return Pipeline.build(withTimestamps(events), config(SEASONAL_END_MS), thresholds());
  }

  private static <T> List<T> collect(DataStream<T> stream) throws Exception {
    List<T> out = new ArrayList<>();
    try (CloseableIterator<T> it = stream.executeAndCollect()) {
      it.forEachRemaining(out::add);
    }
    return out;
  }

  @Test
  void errorSpikeProducesOneFiringAlert() throws Exception {
    List<AlertEvent> alerts = collect(build().alerts());

    List<AlertEvent> firing = alerts.stream().filter(a -> a.state().equals("firing")).toList();
    assertThat(firing).hasSize(1);
    assertThat(firing.get(0).signal()).isEqualTo("error_rate");
    assertThat(firing.get(0).startsAtMs()).isEqualTo(600_000L);
  }

  @Test
  void aggregatesStopAtReplayEnd() throws Exception {
    List<MinuteAgg> aggs = collect(build().aggregates());

    assertThat(aggs).hasSize(15);
    assertThat(aggs.stream().map(MinuteAgg::minuteMs).sorted().toList())
        .containsExactlyElementsOf(
            java.util.stream.LongStream.range(0, 15).map(m -> m * MINUTE_MS).boxed().toList());
  }

  @Test
  void scoresCoverEveryKeyMinuteAndSignal() throws Exception {
    List<ScoreEvent> scores = collect(build().scores());

    List<Tuple> expected = new ArrayList<>();
    for (long m = 0; m < 15; m++) {
      for (Signal s : Signal.values()) {
        expected.add(tuple("test", KEY, m * MINUTE_MS, s.wire()));
      }
    }
    assertThat(scores).hasSize(45);
    assertThat(scores)
        .extracting(ScoreEvent::runId, ScoreEvent::key, ScoreEvent::minuteMs, ScoreEvent::signal)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  void seasonalAlertFiresAfterTwoWeeksOfHistory() throws Exception {
    List<AlertEvent> alerts = collect(buildSeasonal().alerts());

    List<AlertEvent> seasonalFiring =
        alerts.stream()
            .filter(a -> a.detector().equals("seasonal") && a.state().equals("firing"))
            .toList();
    assertThat(seasonalFiring)
        .singleElement()
        .satisfies(
            a -> {
              assertThat(a.setting()).isEqualTo("k4");
              assertThat(a.signal()).isEqualTo("error_rate");
              assertThat(a.key()).isEqualTo(KEY);
              assertThat(a.startsAtMs()).isEqualTo(ERRORS_FROM_MS);
            });
  }
}
