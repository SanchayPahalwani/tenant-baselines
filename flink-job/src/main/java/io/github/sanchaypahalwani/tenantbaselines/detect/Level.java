package io.github.sanchaypahalwani.tenantbaselines.detect;

import io.github.sanchaypahalwani.tenantbaselines.model.Signal;

/** A fire level and a looser resolve level for one signal. */
public record Level(double fire, double resolve) implements java.io.Serializable {

  public Judgement judge(Signal s, double v) {
    if (Double.isNaN(v)) {
      return Judgement.UNSCORED;
    }
    if (s.direction() == Signal.Direction.UP) {
      if (v >= fire) {
        return Judgement.BREACH;
      }
      return v < resolve ? Judgement.CALM : Judgement.NEITHER;
    }
    if (v <= fire) {
      return Judgement.BREACH;
    }
    return v > resolve ? Judgement.CALM : Judgement.NEITHER;
  }
}
