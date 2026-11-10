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
        TenantBaselinesJobTest.class.getResourceAsStream("/thresholds-demo.json")) {
      return ThresholdsFile.load(in);
    }
  }

  /**
   * Generated operator ids depend on the operator's place in the graph, so without a uid an
   * operator added upstream in a later milestone would stop older checkpoints from restoring.
   */
  @Test
  void everyOperatorOfTheLiveJobHasAUid() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    TenantBaselinesJob.addTo(env, JobConfig.parse(new String[] {"--mode", "live"}), thresholds());
    StreamGraph graph = env.getStreamGraph();

    assertThat(graph.getStreamNodes())
        .isNotEmpty()
        .allSatisfy(
            node ->
                assertThat(node.getTransformationUID())
                    .as("uid of %s", node.getOperatorName())
                    .isNotNull());
    assertThat(graph.getStreamNodes().stream().map(StreamNode::getTransformationUID))
        .contains("events-source", "minute-aggregator", "scorer", "late-events");
  }
}
