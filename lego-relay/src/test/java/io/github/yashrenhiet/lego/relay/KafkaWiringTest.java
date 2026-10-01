package io.github.yashrenhiet.lego.relay;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.yashrenhiet.lego.client.TestDatabase;
import io.github.yashrenhiet.lego.relay.delivery.RelayEngine;
import io.github.yashrenhiet.lego.relay.delivery.SinkRegistry;
import io.github.yashrenhiet.lego.relay.sink.KafkaSink;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Checks that a Kafka destination wires up (no broker needed until the first send). */
@SpringBootTest
class KafkaWiringTest {

  @MockitoBean RelayEngine relayEngine; // don't start polling

  @Autowired SinkRegistry sinks;

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", TestDatabase::jdbcUrl);
    registry.add("spring.datasource.username", () -> "postgres");
    registry.add("spring.datasource.password", () -> "postgres");
    registry.add("lego.destinations.orders.type", () -> "KAFKA");
    registry.add("lego.destinations.orders.kafka.topic", () -> "orders.v1");
  }

  @Test
  void kafkaDestinationIsBackedByKafkaSink() {
    assertThat(sinks.forDestination("orders")).isInstanceOf(KafkaSink.class);
  }
}
