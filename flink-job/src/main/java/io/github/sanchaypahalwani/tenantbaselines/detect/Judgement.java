package io.github.sanchaypahalwani.tenantbaselines.detect;

/** How one minute's signal value compares with a threshold level. */
public enum Judgement {
  BREACH,
  CALM,
  NEITHER,
  UNSCORED
}
