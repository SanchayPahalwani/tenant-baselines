package io.github.sanchaypahalwani.tenantbaselines.group;

/**
 * A regional alert while it fires: the start of the minute that promoted it, and how many minutes
 * in a row its share has been under the resolve share. This is a Flink POJO: keep it public with
 * public non-final fields.
 */
public class RegionalAlert {

  public long startedAtMs;
  public int underStreak;

  public RegionalAlert() {}

  public RegionalAlert(long startedAtMs) {
    this.startedAtMs = startedAtMs;
  }
}
