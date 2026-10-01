package io.github.yashrenhiet.lego.relay.admin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.yashrenhiet.lego.relay.RelayFixture;
import io.github.yashrenhiet.lego.relay.admin.AdminRepository.DeadEvent;
import io.github.yashrenhiet.lego.relay.admin.AdminRepository.DestinationStats;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AdminRepositoryTest {

  private RelayFixture fixture;
  private AdminRepository admin;

  @BeforeEach
  void setUp() {
    fixture = new RelayFixture();
    admin = new AdminRepository(fixture.jdbc);
  }

  @Test
  void statsCountPendingAndDeadPerDestination() {
    fixture.write("orders", "a", "1");
    fixture.write("orders", "b", "2");
    fixture.write("webhook", "c", "3");
    kill("orders", "b");

    assertThat(admin.stats())
        .extracting(DestinationStats::destination, DestinationStats::pending, DestinationStats::dead)
        .containsExactly(
            org.assertj.core.groups.Tuple.tuple("orders", 1L, 1L),
            org.assertj.core.groups.Tuple.tuple("webhook", 1L, 0L));
  }

  @Test
  void listsDeadEventsWithOptionalDestinationFilterAndKeysetPaging() {
    fixture.write("orders", "a", "1");
    fixture.write("orders", "b", "2");
    fixture.write("webhook", "c", "3");
    kill("orders", "a");
    kill("orders", "b");
    kill("webhook", "c");

    assertThat(admin.deadEvents(null, 0, 10)).hasSize(3);
    assertThat(admin.deadEvents("webhook", 0, 10)).extracting(DeadEvent::eventKey).containsExactly("c");

    List<DeadEvent> firstPage = admin.deadEvents("orders", 0, 1);
    List<DeadEvent> secondPage = admin.deadEvents("orders", firstPage.getLast().id(), 1);
    assertThat(firstPage).extracting(DeadEvent::eventKey).containsExactly("a");
    assertThat(secondPage).extracting(DeadEvent::eventKey).containsExactly("b");
  }

  @Test
  void replayRequeuesWithFreshRetryBudget() {
    fixture.write("orders", "a", "1");
    UUID id = kill("orders", "a");

    assertThat(admin.replay(List.of(id))).isEqualTo(1);

    assertThat(fixture.count("DEAD")).isZero();
    int attempts = fixture.jdbc.sql("SELECT attempts FROM lego_outbox WHERE event_id = :id")
        .param("id", id).query(Integer.class).single();
    assertThat(attempts).isZero();
    assertThat(fixture.outbox.findDeliverable(fixture.partitionOf("a"), 10)).hasSize(1);
  }

  @Test
  void discardDeletesOnlyDeadEvents() {
    fixture.write("orders", "a", "1");
    fixture.write("orders", "b", "2");
    UUID dead = kill("orders", "a");
    UUID pending = fixture.jdbc.sql("SELECT event_id FROM lego_outbox WHERE event_key = 'b'")
        .query(UUID.class).single();

    assertThat(admin.discard(List.of(dead, pending))).isEqualTo(1);

    assertThat(fixture.count("DEAD")).isZero();
    assertThat(fixture.count("PENDING")).isEqualTo(1);
  }

  private UUID kill(String destination, String key) {
    return fixture.jdbc.sql(
            """
            UPDATE lego_outbox SET status = 'DEAD', attempts = 3, last_error = 'boom'
             WHERE destination = :d AND event_key = :k RETURNING event_id
            """)
        .param("d", destination)
        .param("k", key)
        .query(UUID.class)
        .single();
  }
}
