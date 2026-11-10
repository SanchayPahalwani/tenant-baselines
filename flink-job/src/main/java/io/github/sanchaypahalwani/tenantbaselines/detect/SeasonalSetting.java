package io.github.sanchaypahalwani.tenantbaselines.detect;

import java.math.BigDecimal;

/**
 * One setting of the seasonal detector: a minute breaches at a score of {@code fireK} or more and
 * is calm below {@code resolveK}. {@code setting} names the setting's alerts.
 */
public record SeasonalSetting(String setting, double fireK, double resolveK)
    implements java.io.Serializable {

  /** Fires at {@code k} and resolves below {@code k / 2}; named "k4" for 4 and "k4.5" for 4.5. */
  public static SeasonalSetting ofK(double k) {
    String name = "k" + BigDecimal.valueOf(k).stripTrailingZeros().toPlainString();
    return new SeasonalSetting(name, k, k / 2);
  }
}
