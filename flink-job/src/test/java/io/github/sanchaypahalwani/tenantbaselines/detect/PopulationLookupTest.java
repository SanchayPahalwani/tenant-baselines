package io.github.sanchaypahalwani.tenantbaselines.detect;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.github.sanchaypahalwani.tenantbaselines.baseline.PopulationBaseline;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.junit.jupiter.api.Test;

class PopulationLookupTest {

  /** Only get is implemented; PopulationLookup reads nothing else. */
  private static final class MapState implements ReadOnlyBroadcastState<String, PopulationBaseline> {
    private final Map<String, PopulationBaseline> map = new HashMap<>();

    void put(PopulationBaseline b) {
      map.put(PopulationLookup.key(b.scope(), b.bucket(), b.week()), b);
    }

    @Override
    public PopulationBaseline get(String key) {
      return map.get(key);
    }

    @Override
    public boolean contains(String key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Map.Entry<String, PopulationBaseline>> immutableEntries() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  private static PopulationBaseline record(String scope, int bucket, long week) {
    return new PopulationBaseline(scope, bucket, week, 0.01, 0.005, 120.0, 20.0, 100);
  }

  @Test
  void findsTheWeekAskedFor() throws Exception {
    MapState state = new MapState();
    PopulationBaseline w4 = record("eu", 7, 4);
    PopulationBaseline w5 = record("eu", 7, 5);
    state.put(w4);
    state.put(w5);
    assertSame(w4, PopulationLookup.find(state, "eu", 7, 4));
    assertSame(w5, PopulationLookup.find(state, "eu", 7, 5));
  }

  @Test
  void rejectsALaterWeek() throws Exception {
    MapState state = new MapState();
    state.put(record("eu", 7, 4));
    state.put(record("eu", 7, 5));
    assertNull(PopulationLookup.find(state, "eu", 7, 3));
  }

  @Test
  void fallsBackToTheNewestEarlierWeek() throws Exception {
    MapState state = new MapState();
    PopulationBaseline w4 = record("eu", 7, 4);
    state.put(w4);
    assertSame(w4, PopulationLookup.find(state, "eu", 7, 6));
  }

  @Test
  void otherBucketOrScopeIsNull() throws Exception {
    MapState state = new MapState();
    state.put(record("eu", 7, 4));
    assertNull(PopulationLookup.find(state, "eu", 8, 4));
    assertNull(PopulationLookup.find(state, "us", 7, 4));
  }
}
