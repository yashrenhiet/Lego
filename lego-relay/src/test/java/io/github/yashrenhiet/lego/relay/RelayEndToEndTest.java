package io.github.yashrenhiet.lego.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import io.github.yashrenhiet.lego.client.OutboxEvent;
import io.github.yashrenhiet.lego.client.OutboxWriter;
import io.github.yashrenhiet.lego.client.TestDatabase;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Boots the whole application against the embedded database and a local HTTP receiver, then
 * writes events the way a producer would and waits for them to arrive.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureObservability
@DirtiesContext // stop the relay engine afterwards so it doesn't steal leases from other tests
class RelayEndToEndTest {

  private static final List<String> received = new CopyOnWriteArrayList<>();
  private static final AtomicInteger failuresToInject = new AtomicInteger();
  private static final HttpServer receiver = startReceiver();

  @Autowired DataSource dataSource;
  @Autowired TestRestTemplate rest;

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    TestDatabase.reset();
    registry.add("spring.datasource.url", TestDatabase::jdbcUrl);
    registry.add("spring.datasource.username", () -> "postgres");
    registry.add("spring.datasource.password", () -> "postgres");
    registry.add("lego.polling.interval", () -> "50ms");
    registry.add("lego.leasing.renew-interval", () -> "1s");
    registry.add("lego.retry.initial-backoff", () -> "100ms");
    registry.add("lego.destinations.webhook.type", () -> "HTTP");
    registry.add("lego.destinations.webhook.http.url",
        () -> "http://localhost:" + receiver.getAddress().getPort() + "/hook");
  }

  @AfterAll
  static void stopReceiver() {
    receiver.stop(0);
  }

  @Test
  void deliversEventsInOrderRetriesFailuresAndExposesStats() throws Exception {
    failuresToInject.set(2); // the first two HTTP calls return 503
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      OutboxWriter writer = new OutboxWriter();
      for (int i = 1; i <= 20; i++) {
        writer.write(connection, OutboxEvent.builder("webhook", "customer-1", "{\"seq\":" + i + "}").build());
      }
      connection.commit();
    }

    awaitUntil(() -> received.size() == 20, Duration.ofSeconds(30));

    assertThat(received)
        .containsExactlyElementsOf(
            IntStream.rangeClosed(1, 20).mapToObj(i -> "{\"seq\":" + i + "}").toList());
    // Delivered events are deleted, so the outbox is empty again.
    assertThat(rest.getForObject("/admin/stats", String.class)).isEqualTo("[]");
    assertThat(rest.getForObject("/actuator/prometheus", String.class)).contains("lego_events_retried_total");
  }

  private static void awaitUntil(BooleanSupplier condition, Duration timeout)
      throws InterruptedException {
    Instant deadline = Instant.now().plus(timeout);
    while (!condition.getAsBoolean()) {
      if (Instant.now().isAfter(deadline)) {
        throw new AssertionError("Timed out; received " + received.size() + " events: " + received);
      }
      Thread.sleep(50);
    }
  }

  private static HttpServer startReceiver() {
    try {
      HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
      server.createContext("/hook", exchange -> {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        int status = failuresToInject.getAndUpdate(n -> Math.max(0, n - 1)) > 0 ? 503 : 200;
        if (status == 200) {
          received.add(body);
        }
        exchange.sendResponseHeaders(status, -1);
        exchange.close();
      });
      server.start();
      return server;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
