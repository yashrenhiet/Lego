package io.github.yashrenhiet.lego.relay.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.yashrenhiet.lego.client.TestDatabase;
import io.github.yashrenhiet.lego.relay.delivery.RelayEngine;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** The data-changing admin endpoints must be opt-in. */
class AdminEndpointsTest {

  static void database(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", TestDatabase::jdbcUrl);
    registry.add("spring.datasource.username", () -> "postgres");
    registry.add("spring.datasource.password", () -> "postgres");
  }

  static Map<String, List<UUID>> body() {
    return Map.of("eventIds", List.of(UUID.randomUUID()));
  }

  @Nested
  @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
  class ByDefault {
    @MockitoBean RelayEngine relayEngine;
    @Autowired TestRestTemplate rest;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
      database(registry);
    }

    @Test
    void readEndpointsWorkButWriteEndpointsAreNotExposed() {
      assertThat(rest.getForEntity("/admin/stats", String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
      assertThat(rest.postForEntity("/admin/dead-events/discard", body(), String.class).getStatusCode())
          .isEqualTo(HttpStatus.NOT_FOUND);
    }
  }

  @Nested
  @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
      properties = "lego.admin.write-enabled=true")
  class WhenEnabled {
    @MockitoBean RelayEngine relayEngine;
    @Autowired TestRestTemplate rest;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
      database(registry);
    }

    @Test
    void writeEndpointsAreExposedAndValidated() {
      assertThat(rest.postForEntity("/admin/dead-events/replay", body(), String.class).getBody())
          .isEqualTo("{\"affected\":0}");
      assertThat(rest.postForEntity("/admin/dead-events/replay", Map.of("eventIds", List.of()), String.class)
              .getStatusCode())
          .isEqualTo(HttpStatus.BAD_REQUEST);
    }
  }
}
