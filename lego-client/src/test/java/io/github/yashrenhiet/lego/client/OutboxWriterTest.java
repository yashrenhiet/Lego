package io.github.yashrenhiet.lego.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutboxWriterTest {

  private final DataSource dataSource = TestDatabase.dataSource();
  private final OutboxWriter writer = new OutboxWriter();

  @BeforeEach
  void setUp() {
    TestDatabase.reset();
  }

  @Test
  void writesEventWithAllFields() throws SQLException {
    Instant later = Instant.now().plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);
    OutboxEvent event =
        OutboxEvent.builder("orders", "order-42", "{\"total\":10}")
            .header("content-type", "application/json")
            .deliverAfter(later)
            .build();

    try (Connection connection = dataSource.getConnection()) {
      writer.write(connection, event);
    }

    try (Connection connection = dataSource.getConnection();
        PreparedStatement query =
            connection.prepareStatement(
                "SELECT destination, event_key, payload, headers->>'content-type', status,"
                    + " attempts, next_attempt_at, partition_no FROM lego_outbox WHERE event_id = ?")) {
      query.setObject(1, event.eventId());
      try (ResultSet row = query.executeQuery()) {
        assertThat(row.next()).isTrue();
        assertThat(row.getString(1)).isEqualTo("orders");
        assertThat(row.getString(2)).isEqualTo("order-42");
        assertThat(row.getString(3)).isEqualTo("{\"total\":10}");
        assertThat(row.getString(4)).isEqualTo("application/json");
        assertThat(row.getString(5)).isEqualTo("PENDING");
        assertThat(row.getInt(6)).isZero();
        assertThat(row.getTimestamp(7).toInstant()).isEqualTo(later);
        assertThat(row.getInt(8)).isBetween(0, 63);
      }
    }
  }

  @Test
  void rolledBackTransactionLeavesNoEvent() throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      writer.write(connection, OutboxEvent.builder("orders", "k", "p").build());
      connection.rollback();
    }

    assertThat(countEvents()).isZero();
  }

  @Test
  void writeAllInsertsEveryEvent() throws SQLException {
    List<OutboxEvent> events =
        List.of(
            OutboxEvent.builder("orders", "a", "1").build(),
            OutboxEvent.builder("orders", "b", "2").build(),
            OutboxEvent.builder("orders", "c", "3").build());

    try (Connection connection = dataSource.getConnection()) {
      writer.writeAll(connection, events);
    }

    assertThat(countEvents()).isEqualTo(3);
  }

  @Test
  void sameKeyAlwaysMapsToSamePartition() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement query =
            connection.prepareStatement("SELECT lego_partition_of(?), lego_partition_of(?)")) {
      query.setString(1, "customer-7");
      query.setString(2, "customer-7");
      try (ResultSet row = query.executeQuery()) {
        row.next();
        assertThat(row.getInt(1)).isEqualTo(row.getInt(2));
      }
    }
  }

  @Test
  void duplicateEventIdIsRejected() throws SQLException {
    UUID id = UUID.randomUUID();
    try (Connection connection = dataSource.getConnection()) {
      writer.write(connection, OutboxEvent.builder("orders", "k", "p").eventId(id).build());
      assertThatThrownBy(
              () ->
                  writer.write(connection, OutboxEvent.builder("orders", "k", "p").eventId(id).build()))
          .isInstanceOf(SQLException.class);
    }
  }

  @Test
  void rejectsBlankRequiredFields() {
    assertThatThrownBy(() -> OutboxEvent.builder(" ", "k", "p").build())
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> OutboxEvent.builder("d", "", "p").build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void escapesHeaderJson() {
    assertThat(OutboxWriter.toJson(Map.of("q", "a\"b\\c\n\u0001")))
        .isEqualTo("{\"q\":\"a\\\"b\\\\c\\n\\u0001\"}");
  }

  private long countEvents() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        ResultSet row = connection.createStatement().executeQuery("SELECT count(*) FROM lego_outbox")) {
      row.next();
      return row.getLong(1);
    }
  }
}
