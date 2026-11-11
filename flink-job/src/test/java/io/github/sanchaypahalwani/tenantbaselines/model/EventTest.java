package io.github.sanchaypahalwani.tenantbaselines.model;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.io.EventJson;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class EventTest {

  @Test
  void tickIsNotAnEvent() {
    Event tick = Event.tick(5);

    assertThat(tick.isTick()).isTrue();
    assertThat(tick.ts()).isEqualTo(5);

    // An empty tenant is still a tenant: no parsed event is a tick.
    Event parsed =
        EventJson.parse(
            "{\"ts\":1,\"tn\":\"\",\"rg\":\"r\",\"un\":\"u\",\"lat\":1.0,\"ok\":true}"
                .getBytes(StandardCharsets.UTF_8));

    assertThat(parsed).isNotNull();
    assertThat(parsed.isTick()).isFalse();
  }
}
