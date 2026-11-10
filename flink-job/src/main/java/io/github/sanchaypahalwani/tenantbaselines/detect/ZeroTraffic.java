package io.github.sanchaypahalwani.tenantbaselines.detect;

/**
 * The zero-traffic rule: a 5-minute sum of 0 where at least {@link #MIN_EXPECTED} requests were
 * expected is a breach, whatever the detector says.
 */
public final class ZeroTraffic {

  public static final double MIN_EXPECTED = 15.0;

  private ZeroTraffic() {}

  /**
   * @param expected5m the ring's expected sum when known, otherwise its trailing expectation
   */
  public static Judgement adjust(Judgement own, double volume5m, double expected5m) {
    if (Double.isNaN(volume5m)) {
      return own;
    }
    if (volume5m == 0 && expected5m >= MIN_EXPECTED) {
      return Judgement.BREACH;
    }
    if (own == Judgement.UNSCORED && !Double.isNaN(expected5m)) {
      return volume5m > 0 ? Judgement.CALM : Judgement.NEITHER;
    }
    return own;
  }
}
