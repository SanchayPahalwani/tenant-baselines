package io.github.sanchaypahalwani.tenantbaselines.detect;

/**
 * Turns a stream of per-minute judgements into fire and resolve transitions. Fires after {@link
 * #FIRE_AFTER} consecutive breaches and resolves after {@link #RESOLVE_AFTER} consecutive calm
 * minutes. Unscored minutes change nothing. This is a Flink POJO: keep it public with public
 * non-final fields.
 */
public class AlertStateMachine {

  public static final int FIRE_AFTER = 3;
  public static final int RESOLVE_AFTER = 5;

  public enum Transition {
    NONE,
    FIRE,
    RESOLVE
  }

  public boolean firing;
  public int breachStreak;
  public int calmStreak;
  public long breachStartMs;
  public long startedAtMs;

  public AlertStateMachine() {}

  public Transition step(Judgement j, long minuteMs) {
    if (j == Judgement.UNSCORED) {
      return Transition.NONE;
    }
    if (!firing) {
      if (j == Judgement.BREACH) {
        if (breachStreak == 0) {
          breachStartMs = minuteMs;
        }
        breachStreak++;
        if (breachStreak >= FIRE_AFTER) {
          firing = true;
          startedAtMs = breachStartMs;
          breachStreak = 0;
          calmStreak = 0;
          return Transition.FIRE;
        }
      } else {
        breachStreak = 0;
      }
      return Transition.NONE;
    }
    if (j == Judgement.CALM) {
      calmStreak++;
      if (calmStreak >= RESOLVE_AFTER) {
        firing = false;
        breachStreak = 0;
        calmStreak = 0;
        return Transition.RESOLVE;
      }
    } else {
      calmStreak = 0;
    }
    return Transition.NONE;
  }
}
