package io.github.sanchaypahalwani.tenantbaselines.baseline;

import io.github.sanchaypahalwani.tenantbaselines.detect.Judgement;
import io.github.sanchaypahalwani.tenantbaselines.model.Signal;

/** Spreads with their floors, scores in spreads, and the judgement of a score. */
public final class Scores {
  private static final double MIN_ERROR_RATE = 0.005;
  private static final double LATENCY_FLOOR = 0.05;

  private Scores() {}

  /** The larger of the scaled MAD and the sampling error of a rate seen over n requests. */
  public static double errorRateSpread(double median, double scaledMad, long n) {
    double p0 = Math.max(median, MIN_ERROR_RATE);
    return Math.max(scaledMad, Math.sqrt(p0 * (1 - p0) / n));
  }

  public static double latencySpread(double median, double scaledMad) {
    return Math.max(scaledMad, LATENCY_FLOOR * median);
  }

  public static double volumeSpread(double scaledMadOfSum, double expected) {
    return Math.max(scaledMadOfSum, Math.sqrt(expected));
  }

  /** Distance from the reference in spreads, positive in the signal's bad direction. */
  public static double score(Signal s, double value, double reference, double spread) {
    if (Double.isNaN(value) || Double.isNaN(reference) || Double.isNaN(spread) || spread <= 0) {
      return Double.NaN;
    }
    return s.direction() == Signal.Direction.UP
        ? (value - reference) / spread
        : (reference - value) / spread;
  }

  public static Judgement judge(double score, double fireK, double resolveK) {
    if (Double.isNaN(score)) {
      return Judgement.UNSCORED;
    }
    if (score >= fireK) {
      return Judgement.BREACH;
    }
    return score < resolveK ? Judgement.CALM : Judgement.NEITHER;
  }

  /** The value at which the score reaches fireK. */
  public static double fireReference(Signal s, double reference, double spread, double fireK) {
    return s.direction() == Signal.Direction.UP
        ? reference + fireK * spread
        : reference - fireK * spread;
  }
}
