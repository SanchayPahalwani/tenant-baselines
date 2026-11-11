package io.github.sanchaypahalwani.tenantbaselines.group;

/**
 * One of a tenant's keys became active ({@code delta} +1) or stopped being active (-1). This is a
 * Flink POJO: keep it public with public non-final fields.
 */
public class ActivityFlip {

  public String tenant;
  public int delta;

  public ActivityFlip() {}

  public ActivityFlip(String tenant, int delta) {
    this.tenant = tenant;
    this.delta = delta;
  }
}
