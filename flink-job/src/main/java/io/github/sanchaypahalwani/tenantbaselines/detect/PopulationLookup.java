package io.github.sanchaypahalwani.tenantbaselines.detect;

import io.github.sanchaypahalwani.tenantbaselines.baseline.PopulationBaseline;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;

/** Keys and reads the population baselines in the broadcast state. */
public final class PopulationLookup {

  private PopulationLookup() {}

  /** Weeks alternate between two keys, so a scope and bucket hold at most two records. */
  public static String key(String scope, int bucket, long week) {
    return scope + "|" + bucket + "|" + Math.floorMod(week, 2);
  }

  /** The scope's newest record for the bucket that is not for a later week, or null. */
  public static PopulationBaseline find(
      ReadOnlyBroadcastState<String, PopulationBaseline> population,
      String scope,
      int bucket,
      long week)
      throws Exception {
    PopulationBaseline best = null;
    for (long w = week - 1; w <= week; w++) {
      PopulationBaseline b = population.get(key(scope, bucket, w));
      if (b != null && b.week() <= week && (best == null || b.week() > best.week())) {
        best = b;
      }
    }
    return best;
  }
}
