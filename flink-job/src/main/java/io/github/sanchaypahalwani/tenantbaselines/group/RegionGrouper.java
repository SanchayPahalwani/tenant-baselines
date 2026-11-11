package io.github.sanchaypahalwani.tenantbaselines.group;

import io.github.sanchaypahalwani.tenantbaselines.detect.AlertIds;
import io.github.sanchaypahalwani.tenantbaselines.detect.Alerts;
import io.github.sanchaypahalwani.tenantbaselines.model.AlertEvent;
import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Groups the tenant alert candidates of one region, keyed by region on both inputs. A firing
 * candidate is held for {@link #HOLD_MS} of event time. When its hold ends, the region has at
 * least {@link #MIN_ACTIVE_TENANTS} active tenants, and at least {@link #PROMOTE_SHARE} of them
 * alert on the same detector, setting and signal, they become one regional alert and the held
 * tenant alerts are absorbed into it. Otherwise the tenant alert is released. A tenant is active
 * while one of its keys in the region has {@link KeyActivity#ACTIVE_REQUESTS} requests in the last
 * hour.
 *
 * <p>A regional alert resolves once its share has been under {@link #RESOLVE_SHARE} for {@link
 * #RESOLVE_AFTER} minutes in a row. The tenant alerts it absorbed that still fire are then held
 * again, and that hold ends like any other.
 *
 * <p>The two inputs and the many keys of a region arrive in no order between two watermarks. So
 * both inputs are only stored here, and everything is applied in {@link #onTimer}, which Flink
 * fires in timestamp order. Every timer sits on the last millisecond of a minute, and a decision
 * at a minute sees the candidates and the activity up to that minute and nothing later. While a
 * regional alert fires there is a timer every minute. Within one timer the candidates, groups and
 * alerts are walked in sorted order, so the events of a minute come out in the same order on every
 * run.
 */
public class RegionGrouper
    extends KeyedCoProcessFunction<String, AlertEvent, MinuteAgg, AlertEvent> {

  public static final long HOLD_MS = 120_000L;
  public static final double PROMOTE_SHARE = 0.5;
  public static final double RESOLVE_SHARE = 0.25;
  public static final int RESOLVE_AFTER = 5;
  public static final int MIN_ACTIVE_TENANTS = 3;

  private static final long MINUTE_MS = 60_000L;
  private static final String REGIONAL_REASON = "regional:";

  private static final Comparator<AlertEvent> CANDIDATE_ORDER =
      Comparator.comparing(AlertEvent::detector)
          .thenComparing(AlertEvent::setting)
          .thenComparing(AlertEvent::signal)
          .thenComparing(AlertEvent::key)
          .thenComparing(AlertEvent::state);

  private final String runId;

  /** The detector and setting whose sent alerts drain units. Not read yet. */
  private final String liveDetector;

  private final String liveSetting;

  /** How long a unit remembers its tenants. Not read yet. */
  private final Duration expiry;

  /** Candidates by the timer that applies them. */
  private transient MapState<Long, List<AlertEvent>> pendingAlerts;

  /** Activity changes by the timer that applies them. */
  private transient MapState<Long, List<ActivityFlip>> pendingFlips;

  /** Tenant alerts by detector/setting/signal|key, from their firing to their resolved. */
  private transient MapState<String, TenantAlert> alerts;

  /** Regional alerts by detector/setting/signal while they fire. */
  private transient MapState<String, RegionalAlert> regional;

  /** Request counts by key. */
  private transient MapState<String, KeyActivity> activity;

  /** Tenant to the number of its active keys in the region; changed only in timers. */
  private transient MapState<String, Integer> activeKeys;

  public RegionGrouper(String runId, String liveDetector, String liveSetting, Duration expiry) {
    this.runId = runId;
    this.liveDetector = liveDetector;
    this.liveSetting = liveSetting;
    this.expiry = expiry;
  }

  /** How many tenants count and the share of them that alert. */
  private record Share(int tenants, double share) {}

  /** A group's detector, setting and signal, read back from its key. */
  private record Group(String detector, String setting, String signal) {

    /** Splits at the first and last slash: detector and signal names have none. */
    static Group of(String key) {
      int first = key.indexOf('/');
      int last = key.lastIndexOf('/');
      return new Group(
          key.substring(0, first), key.substring(first + 1, last), key.substring(last + 1));
    }
  }

  @Override
  public void open(OpenContext openContext) {
    pendingAlerts =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "pending-alerts",
                    Types.LONG,
                    Types.LIST(TypeInformation.of(AlertEvent.class))));
    pendingFlips =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "pending-flips",
                    Types.LONG,
                    Types.LIST(TypeInformation.of(ActivityFlip.class))));
    alerts =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "alerts", Types.STRING, TypeInformation.of(TenantAlert.class)));
    regional =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "regional", Types.STRING, TypeInformation.of(RegionalAlert.class)));
    activity =
        getRuntimeContext()
            .getMapState(
                new MapStateDescriptor<>(
                    "activity", Types.STRING, TypeInformation.of(KeyActivity.class)));
    activeKeys =
        getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("active-keys", Types.STRING, Types.INT));
  }

  /** The last millisecond of the minute of ts. */
  static long minuteEnd(long ts) {
    return ts - Math.floorMod(ts, MINUTE_MS) + MINUTE_MS - 1;
  }

  /** Stores the candidate for the end of the minute that decided it. */
  @Override
  public void processElement1(AlertEvent c, Context ctx, Collector<AlertEvent> out)
      throws Exception {
    long t = minuteEnd(ctx.timestamp());
    append(pendingAlerts, t, c);
    ctx.timerService().registerEventTimeTimer(t);
  }

  /**
   * Updates the key's ring at once, since a key's minutes arrive in order. Only a change of the
   * key's activity waits for the end of its minute.
   */
  @Override
  public void processElement2(MinuteAgg m, Context ctx, Collector<AlertEvent> out)
      throws Exception {
    KeyActivity k = activity.get(m.key());
    if (k == null) {
      k = new KeyActivity();
      k.tenant = m.tenant();
    }
    boolean was = k.active;
    k.add(m.minuteMs(), m.count());
    if (k.active != was) {
      // From the minute, not ctx.timestamp(): aggregates read back may carry no record timestamp.
      long t = minuteEnd(m.minuteMs());
      append(pendingFlips, t, new ActivityFlip(m.tenant(), k.active ? 1 : -1));
      ctx.timerService().registerEventTimeTimer(t);
    }
    if (k.sum == 0) {
      activity.remove(m.key());
    } else {
      activity.put(m.key(), k);
    }
  }

  /** Applies the minute that ends at t; its events are stamped with the next minute's start. */
  @Override
  public void onTimer(long t, OnTimerContext ctx, Collector<AlertEvent> out) throws Exception {
    long at = t + 1;
    String region = ctx.getCurrentKey();
    TimerService timers = ctx.timerService();
    // 1. The minute's activity changes.
    applyFlips(t);
    // 2. The minute's candidates, in a fixed order.
    applyCandidates(t, region, timers, out);
    // Steps 3 and 4 change the alerts' status in this map and in state alike, so step 4 sees the
    // holds that step 3 starts.
    SortedMap<String, SortedMap<String, TenantAlert>> byGroup = alertsByGroup();
    // 3. Regional alerts under a quarter for five minutes resolve.
    resolveRegional(t, at, region, byGroup, timers, out);
    // 4. Holds that end now: promote or release.
    endHolds(t, at, region, byGroup, out);
    // 5. Unit status: to come.
    // 6. The next minute's timer while a regional alert fires, so that its streak counts minutes.
    // None at the final watermark of a bounded input, where timers would follow each other forever.
    if (!regional.isEmpty() && timers.currentWatermark() != Long.MAX_VALUE) {
      timers.registerEventTimeTimer(t + MINUTE_MS);
    }
  }

  private void applyFlips(long t) throws Exception {
    List<ActivityFlip> flips = pendingFlips.get(t);
    if (flips == null) {
      return;
    }
    for (ActivityFlip f : flips) {
      Integer keys = activeKeys.get(f.tenant);
      int next = (keys == null ? 0 : keys) + f.delta;
      if (next == 0) {
        activeKeys.remove(f.tenant);
      } else {
        activeKeys.put(f.tenant, next);
      }
    }
    pendingFlips.remove(t);
  }

  private void applyCandidates(
      long t, String region, TimerService timers, Collector<AlertEvent> out) throws Exception {
    List<AlertEvent> candidates = pendingAlerts.get(t);
    if (candidates == null) {
      return;
    }
    List<AlertEvent> sorted = new ArrayList<>(candidates);
    sorted.sort(CANDIDATE_ORDER);
    for (AlertEvent c : sorted) {
      apply(c, t, region, timers, out);
    }
    pendingAlerts.remove(t);
  }

  /**
   * A firing candidate is held, or absorbed at once while its group's regional alert fires; a
   * repeat changes nothing. A resolved one ends the tenant alert, and comes out unchanged only if
   * the alert was sent or the grouper has no alert for it.
   */
  private void apply(
      AlertEvent c, long t, String region, TimerService timers, Collector<AlertEvent> out)
      throws Exception {
    String group = group(c.detector(), c.setting(), c.signal());
    String id = group + "|" + c.key();
    if (Alerts.FIRING.equals(c.state())) {
      if (alerts.contains(id)) {
        return;
      }
      TenantAlert a = TenantAlert.of(c);
      a.activeAtFire = activeKeys.contains(c.tenant());
      RegionalAlert r = regional.get(group);
      if (r != null) {
        a.status = TenantAlert.ABSORBED;
        out.collect(absorbed(a, r, region, c.eventTimeMs()));
      } else {
        a.status = TenantAlert.HELD;
        a.heldUntilMs = t + HOLD_MS;
        timers.registerEventTimeTimer(a.heldUntilMs);
      }
      alerts.put(id, a);
      return;
    }
    TenantAlert a = alerts.get(id);
    alerts.remove(id);
    if (a == null || TenantAlert.SENT.equals(a.status)) {
      out.collect(c);
    }
  }

  /**
   * Counts, for each regional alert, the minutes in a row its share has been under {@link
   * #RESOLVE_SHARE}. After {@link #RESOLVE_AFTER} it resolves, and each tenant alert it absorbed,
   * all of which still fire, is held again.
   */
  private void resolveRegional(
      long t,
      long at,
      String region,
      SortedMap<String, SortedMap<String, TenantAlert>> byGroup,
      TimerService timers,
      Collector<AlertEvent> out)
      throws Exception {
    SortedMap<String, RegionalAlert> firing = new TreeMap<>();
    for (Map.Entry<String, RegionalAlert> e : regional.entries()) {
      firing.put(e.getKey(), e.getValue());
    }
    for (Map.Entry<String, RegionalAlert> g : firing.entrySet()) {
      RegionalAlert r = g.getValue();
      SortedMap<String, TenantAlert> groupAlerts =
          byGroup.getOrDefault(g.getKey(), Collections.emptySortedMap());
      Share s = share(groupAlerts.values());
      r.underStreak = s.share() < RESOLVE_SHARE ? r.underStreak + 1 : 0;
      if (r.underStreak < RESOLVE_AFTER) {
        regional.put(g.getKey(), r);
        continue;
      }
      Group group = Group.of(g.getKey());
      out.collect(
          Alerts.of(
              runId,
              group.detector(),
              group.setting(),
              Alerts.REGION,
              region,
              "",
              region,
              "",
              group.signal(),
              Alerts.RESOLVED,
              r.startedAtMs,
              at,
              s.share(),
              RESOLVE_SHARE,
              null));
      regional.remove(g.getKey());
      for (Map.Entry<String, TenantAlert> e : groupAlerts.entrySet()) {
        TenantAlert a = e.getValue();
        if (TenantAlert.ABSORBED.equals(a.status)) {
          a.status = TenantAlert.HELD;
          a.heldUntilMs = t + HOLD_MS;
          alerts.put(e.getKey(), a);
          timers.registerEventTimeTimer(a.heldUntilMs);
        }
      }
    }
  }

  /**
   * For each group with a hold that ends by t: promote to a regional alert, which absorbs every
   * held alert of the group, or release the alerts whose hold ended.
   */
  private void endHolds(
      long t,
      long at,
      String region,
      SortedMap<String, SortedMap<String, TenantAlert>> byGroup,
      Collector<AlertEvent> out)
      throws Exception {
    for (Map.Entry<String, SortedMap<String, TenantAlert>> g : byGroup.entrySet()) {
      SortedMap<String, TenantAlert> groupAlerts = g.getValue();
      if (groupAlerts.values().stream().noneMatch(a -> due(a, t))) {
        continue;
      }
      Share s = share(groupAlerts.values());
      if (s.tenants() >= MIN_ACTIVE_TENANTS && s.share() >= PROMOTE_SHARE) {
        RegionalAlert r = new RegionalAlert(at - MINUTE_MS);
        regional.put(g.getKey(), r);
        TenantAlert first = groupAlerts.get(groupAlerts.firstKey());
        out.collect(
            Alerts.of(
                runId,
                first.detector,
                first.setting,
                Alerts.REGION,
                region,
                "",
                region,
                "",
                first.signal,
                Alerts.FIRING,
                r.startedAtMs,
                at,
                s.share(),
                PROMOTE_SHARE,
                null));
        for (Map.Entry<String, TenantAlert> e : groupAlerts.entrySet()) {
          TenantAlert a = e.getValue();
          if (TenantAlert.HELD.equals(a.status)) {
            a.status = TenantAlert.ABSORBED;
            alerts.put(e.getKey(), a);
            out.collect(absorbed(a, r, region, at));
          }
        }
      } else {
        for (Map.Entry<String, TenantAlert> e : groupAlerts.entrySet()) {
          TenantAlert a = e.getValue();
          if (due(a, t)) {
            a.status = TenantAlert.SENT;
            alerts.put(e.getKey(), a);
            out.collect(
                Alerts.of(
                    runId,
                    a.detector,
                    a.setting,
                    Alerts.TENANT,
                    a.key,
                    a.tenant,
                    region,
                    a.unit,
                    a.signal,
                    Alerts.FIRING,
                    a.startsAtMs,
                    at,
                    a.value,
                    a.reference,
                    a.reason));
          }
        }
      }
    }
  }

  private static boolean due(TenantAlert a, long t) {
    return TenantAlert.HELD.equals(a.status) && a.heldUntilMs <= t;
  }

  /** The tenant alerts by group, then by id, both sorted. */
  private SortedMap<String, SortedMap<String, TenantAlert>> alertsByGroup() throws Exception {
    SortedMap<String, SortedMap<String, TenantAlert>> groups = new TreeMap<>();
    for (Map.Entry<String, TenantAlert> e : alerts.entries()) {
      TenantAlert a = e.getValue();
      groups
          .computeIfAbsent(group(a.detector, a.setting, a.signal), g -> new TreeMap<>())
          .put(e.getKey(), a);
    }
    return groups;
  }

  /**
   * The tenants that count are the active ones and those with an alert of the group that fired
   * while they were active; the share is the part of them with an alert of the group. A tenant
   * counts once, however many of its keys alert.
   */
  private Share share(Collection<TenantAlert> groupAlerts) throws Exception {
    Set<String> counted = new HashSet<>();
    for (String tenant : activeKeys.keys()) {
      counted.add(tenant);
    }
    for (TenantAlert a : groupAlerts) {
      if (a.activeAtFire) {
        counted.add(a.tenant);
      }
    }
    Set<String> alerting = new HashSet<>();
    for (TenantAlert a : groupAlerts) {
      if (counted.contains(a.tenant)) {
        alerting.add(a.tenant);
      }
    }
    int n = counted.size();
    return new Share(n, n == 0 ? 0.0 : (double) alerting.size() / n);
  }

  /** The tenant alert's own event, pointing at the regional alert that absorbed it. */
  private AlertEvent absorbed(TenantAlert a, RegionalAlert r, String region, long eventTimeMs) {
    String regionalId =
        AlertIds.of(a.detector, a.setting, Alerts.REGION, region, a.signal, r.startedAtMs);
    return Alerts.of(
        runId,
        a.detector,
        a.setting,
        Alerts.TENANT,
        a.key,
        a.tenant,
        region,
        a.unit,
        a.signal,
        Alerts.ABSORBED,
        a.startsAtMs,
        eventTimeMs,
        a.value,
        a.reference,
        REGIONAL_REASON + regionalId);
  }

  private static String group(String detector, String setting, String signal) {
    return detector + "/" + setting + "/" + signal;
  }

  private static <T> void append(MapState<Long, List<T>> state, long t, T item) throws Exception {
    List<T> items = state.get(t);
    if (items == null) {
      items = new ArrayList<>();
    }
    items.add(item);
    state.put(t, items);
  }
}
