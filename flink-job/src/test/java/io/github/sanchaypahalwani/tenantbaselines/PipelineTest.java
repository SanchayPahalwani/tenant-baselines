package io.github.sanchaypahalwani.tenantbaselines;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.detect.ThresholdSet;
import io.github.sanchaypahalwani.tenantbaselines.detect.ThresholdsFile;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class PipelineTest {

  private static final long MINUTE_MS = 60_000L;
  private static final long REPLAY_END_MS = 15 * MINUTE_MS;

  @RegisterExtension
  static final MiniClusterExtension CLUSTER =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(2)
              .build());

  private static JobConfig config() {
    return JobConfig.parse(
        new String[] {"--run-id", "test", "--mode", "replay", "--replay-end-ms", "" + REPLAY_END_MS});
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

  private static Pipeline.Outputs build() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Event> stream =
        env.fromData(events())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                    .withTimestampAssigner((e, t) -> e.ts()));
    return Pipeline.build(stream, config(), thresholds());
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
}
