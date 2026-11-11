package io.github.sanchaypahalwani.tenantbaselines;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.detect.ThresholdSet;
import io.github.sanchaypahalwani.tenantbaselines.detect.ThresholdsFile;
import java.io.InputStream;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;

class TenantBaselinesJobTest {

  private static List<ThresholdSet> thresholds() throws Exception {
    try (InputStream in =
        TenantBaselinesJobTest.class.getResourceAsStream("/thresholds-test.json")) {
      return ThresholdsFile.load(in);
    }
  }

  private static StreamGraph graph(String... args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    TenantBaselinesJob.addTo(env, JobConfig.parse(args), thresholds());
    return env.getStreamGraph();
  }

  private static List<String> uids(StreamGraph graph) {
    return graph.getStreamNodes().stream().map(StreamNode::getTransformationUID).toList();
  }

  /**
   * Generated operator ids depend on the operator's place in the graph, so without a uid an
   * operator added upstream in a later milestone would stop older checkpoints from restoring.
   */
  @Test
  void everyOperatorOfTheLiveJobHasAUid() throws Exception {
    StreamGraph graph = graph("--mode", "live");

    assertThat(graph.getStreamNodes())
        .isNotEmpty()
        .allSatisfy(
            node ->
                assertThat(node.getTransformationUID())
                    .as("uid of %s", node.getOperatorName())
                    .isNotNull());
    assertThat(uids(graph))
        .contains(
            "events-source",
            "ticks-source",
            "drop-ticks",
            "minute-aggregator",
            "population-baselines",
            "scorer",
            "scores-sink",
            "region-grouper",
            "unit-status-sink",
            "late-events");
  }

  @Test
  void replayJobHasNoTicks() throws Exception {
    assertThat(uids(graph("--ns", "eval", "--mode", "replay")))
        .doesNotContain("ticks-source", "drop-ticks");
  }

  @Test
  void scoresAreSunkOnlyWhenEmitted() throws Exception {
    assertThat(uids(graph("--ns", "eval", "--mode", "replay"))).doesNotContain("scores-sink");
    assertThat(uids(graph("--ns", "eval", "--mode", "replay", "--scores", "true")))
        .contains("scores-sink");
  }
}
