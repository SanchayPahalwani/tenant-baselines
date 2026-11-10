package io.github.sanchaypahalwani.tenantbaselines.detect;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SeasonalSettingTest {

  @Test
  void ofKNamesTheSettingAndResolvesAtHalf() {
    assertThat(SeasonalSetting.ofK(4)).isEqualTo(new SeasonalSetting("k4", 4, 2));
    assertThat(SeasonalSetting.ofK(4.5)).isEqualTo(new SeasonalSetting("k4.5", 4.5, 2.25));
  }
}
