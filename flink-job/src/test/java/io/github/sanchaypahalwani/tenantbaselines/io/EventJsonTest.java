package io.github.sanchaypahalwani.tenantbaselines.io;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sanchaypahalwani.tenantbaselines.model.Event;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

class EventJsonTest {

  @Test
  void parsesWireFormat() {
    byte[] json =
        "{\"ts\":60000,\"tn\":\"t01\",\"rg\":\"eu-central\",\"un\":\"eu-central-s1\",\"lat\":83.2,\"ok\":false}"
            .getBytes(StandardCharsets.UTF_8);

    Event event = EventJson.parse(json);

    assertThat(event).isEqualTo(new Event(60000, "t01", "eu-central", "eu-central-s1", 83.2, false));
    assertThat(event.key()).isEqualTo("t01|eu-central|eu-central-s1");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "not json",
        "{}",
        "{\"ts\":\"x\",\"tn\":\"t01\",\"rg\":\"r\",\"un\":\"u\",\"lat\":1.0,\"ok\":true}",
        "{\"ts\":1,\"tn\":\"t01\",\"rg\":\"r\",\"un\":\"u\",\"ok\":true}"
      })
  void returnsNullOnBadInput(String json) {
    assertThat(EventJson.parse(json.getBytes(StandardCharsets.UTF_8))).isNull();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"ts\":1,\"tn\":\"a|b\",\"rg\":\"r\",\"un\":\"u\",\"lat\":1.0,\"ok\":true}",
        "{\"ts\":1,\"tn\":\"t\",\"rg\":\"e|u\",\"un\":\"u\",\"lat\":1.0,\"ok\":true}",
        "{\"ts\":1,\"tn\":\"t\",\"rg\":\"r\",\"un\":\"u|1\",\"lat\":1.0,\"ok\":true}"
      })
  void rejectsPipeInDimensions(String json) {
    assertThat(EventJson.parse(json.getBytes(StandardCharsets.UTF_8))).isNull();
  }
}
