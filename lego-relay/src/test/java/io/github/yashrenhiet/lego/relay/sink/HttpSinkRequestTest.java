package io.github.yashrenhiet.lego.relay.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.github.yashrenhiet.lego.relay.config.LegoProperties;
import io.github.yashrenhiet.lego.relay.delivery.DeliveryException;
import io.github.yashrenhiet.lego.relay.outbox.OutboxRecord;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class HttpSinkRequestTest {

  private final HttpSink sink =
      new HttpSink(
          HttpClient.newHttpClient(),
          new LegoProperties.Http(
              URI.create("http://localhost/hook"),
              "POST",
              Map.of("X-Static", "s"),
              Duration.ofSeconds(1),
              Duration.ofSeconds(1)));

  @Test
  void eventIdCannotBeOverriddenByEventHeaders() throws DeliveryException {
    UUID id = UUID.randomUUID();

    HttpRequest request = sink.buildRequest(event(id, Map.of("Lego-Event-Id", "spoofed", "X-Trace", "t")));

    assertThat(request.headers().allValues("Lego-Event-Id")).containsExactly(id.toString());
    assertThat(request.headers().firstValue("X-Trace")).contains("t");
    assertThat(request.headers().firstValue("X-Static")).contains("s");
  }

  @Test
  void restrictedHeaderIsAPermanentFailure() {
    DeliveryException e =
        catchThrowableOfType(
            DeliveryException.class, () -> sink.buildRequest(event(UUID.randomUUID(), Map.of("Host", "evil"))));

    assertThat(e).isNotNull();
    assertThat(e.isPermanent()).isTrue();
  }

  private static OutboxRecord event(UUID id, Map<String, String> headers) {
    return new OutboxRecord(1, id, "webhook", "k", 0, headers, "{}", 0, Instant.now());
  }
}
