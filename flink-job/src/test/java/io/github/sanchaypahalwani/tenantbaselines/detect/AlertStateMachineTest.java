package io.github.sanchaypahalwani.tenantbaselines.detect;

import static io.github.sanchaypahalwani.tenantbaselines.detect.AlertStateMachine.Transition.FIRE;
import static io.github.sanchaypahalwani.tenantbaselines.detect.AlertStateMachine.Transition.NONE;
import static io.github.sanchaypahalwani.tenantbaselines.detect.AlertStateMachine.Transition.RESOLVE;
import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.BREACH;
import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.CALM;
import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.NEITHER;
import static io.github.sanchaypahalwani.tenantbaselines.detect.Judgement.UNSCORED;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class AlertStateMachineTest {

  private static List<AlertStateMachine.Transition> run(
      AlertStateMachine m, long startMinute, Judgement... js) {
    List<AlertStateMachine.Transition> out = new ArrayList<>();
    long minute = startMinute;
    for (Judgement j : js) {
      out.add(m.step(j, minute * 60_000));
      minute++;
    }
    return out;
  }

  @Test
  void firesAfterThreeBreaches() {
    var m = new AlertStateMachine();
    assertThat(run(m, 4, BREACH, BREACH, BREACH)).containsExactly(NONE, NONE, FIRE);
    assertThat(m.firing).isTrue();
    assertThat(m.startedAtMs).isEqualTo(4 * 60_000);
  }

  @Test
  void neitherResetsBreachStreak() {
    var m = new AlertStateMachine();
    assertThat(run(m, 0, BREACH, BREACH, NEITHER, BREACH, BREACH)).doesNotContain(FIRE);
  }

  @Test
  void unscoredPausesStreak() {
    var m = new AlertStateMachine();
    assertThat(run(m, 0, BREACH, UNSCORED, BREACH, BREACH)).containsExactly(NONE, NONE, NONE, FIRE);
  }

  @Test
  void unscoredPausesCalmStreak() {
    var m = new AlertStateMachine();
    run(m, 0, BREACH, BREACH, BREACH);
    assertThat(run(m, 3, CALM, CALM, UNSCORED, CALM, CALM, CALM))
        .containsExactly(NONE, NONE, NONE, NONE, NONE, RESOLVE);
  }

  @Test
  void resolvesAfterFiveCalm() {
    var m = new AlertStateMachine();
    run(m, 0, BREACH, BREACH, BREACH);
    var out = run(m, 3, CALM, CALM, CALM, CALM, NEITHER, CALM, CALM, CALM, CALM, CALM);
    assertThat(out.subList(0, 9)).containsOnly(NONE);
    assertThat(out.get(9)).isEqualTo(RESOLVE);
    assertThat(m.firing).isFalse();
  }

  @Test
  void refireGetsNewStart() {
    var m = new AlertStateMachine();
    run(m, 0, BREACH, BREACH, BREACH, CALM, CALM, CALM, CALM, CALM);
    assertThat(m.firing).isFalse();
    assertThat(run(m, 20, BREACH, BREACH, BREACH)).containsExactly(NONE, NONE, FIRE);
    assertThat(m.startedAtMs).isEqualTo(20 * 60_000);
  }
}
