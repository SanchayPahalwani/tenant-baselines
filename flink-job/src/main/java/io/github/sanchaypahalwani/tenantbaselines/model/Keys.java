package io.github.sanchaypahalwani.tenantbaselines.model;

/** Joins and splits the tenant, region and unit parts of a series key. */
public final class Keys {

  private static final String SEPARATOR = "|";

  private Keys() {}

  public static String of(String tenant, String region, String unit) {
    return tenant + SEPARATOR + region + SEPARATOR + unit;
  }

  /** Returns {tenant, region, unit}. */
  public static String[] split(String key) {
    return key.split("\\|", 3);
  }
}
