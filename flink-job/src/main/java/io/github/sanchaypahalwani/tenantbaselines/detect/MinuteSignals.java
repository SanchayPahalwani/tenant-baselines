package io.github.sanchaypahalwani.tenantbaselines.detect;

import io.github.sanchaypahalwani.tenantbaselines.model.MinuteAgg;
import io.github.sanchaypahalwani.tenantbaselines.model.Signal;

/** The signal values for one key and minute. NaN means the signal cannot be scored. */
public record MinuteSignals(double errorRate, double latencyP95, double volume5m) {

  public static final long MIN_REQUESTS = 20;

  public static MinuteSignals of(MinuteAgg a, double volume5m) {
    if (a.count() < MIN_REQUESTS) {
      return new MinuteSignals(Double.NaN, Double.NaN, volume5m);
    }
    double latency = a.p95LatencyMs() == null ? Double.NaN : a.p95LatencyMs();
    return new MinuteSignals((double) a.errors() / a.count(), latency, volume5m);
  }

  public double get(Signal s) {
    return switch (s) {
      case ERROR_RATE -> errorRate;
      case LATENCY_P95 -> latencyP95;
      case VOLUME_5M -> volume5m;
    };
  }
}
