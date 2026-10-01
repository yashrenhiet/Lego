package io.github.yashrenhiet.lego.client;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Writes events to the outbox using the caller's JDBC connection, so the event commits (or rolls
 * back) atomically with the caller's business data.
 *
 * <p>This class never commits, rolls back or closes the connection; transaction control stays
 * with the caller. With Spring, obtain the transaction-bound connection via {@code
 * DataSourceUtils.getConnection(dataSource)}.
 *
 * <p>Instances are stateless and thread-safe.
 */
public final class OutboxWriter {

  static final String INSERT_SQL =
      """
      INSERT INTO lego_outbox (event_id, destination, event_key, headers, payload, next_attempt_at)
      VALUES (?, ?, ?, ?::jsonb, ?, COALESCE(?, now()))
      """;

  /** Writes a single event and returns its id. */
  public UUID write(Connection connection, OutboxEvent event) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
      bind(statement, event);
      statement.executeUpdate();
    }
    return event.eventId();
  }

  /** Writes all events in one JDBC batch. */
  public void writeAll(Connection connection, List<OutboxEvent> events) throws SQLException {
    if (events.isEmpty()) {
      return;
    }
    try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
      for (OutboxEvent event : events) {
        bind(statement, event);
        statement.addBatch();
      }
      statement.executeBatch();
    }
  }

  private static void bind(PreparedStatement statement, OutboxEvent event) throws SQLException {
    statement.setObject(1, event.eventId());
    statement.setString(2, event.destination());
    statement.setString(3, event.key());
    statement.setString(4, toJson(event.headers()));
    statement.setString(5, event.payload());
    if (event.deliverAfter() == null) {
      statement.setNull(6, Types.TIMESTAMP_WITH_TIMEZONE);
    } else {
      statement.setObject(6, event.deliverAfter().atOffset(ZoneOffset.UTC));
    }
  }

  /** Minimal JSON encoding for a flat string map; keeps this module dependency-free. */
  static String toJson(Map<String, String> headers) {
    StringBuilder json = new StringBuilder("{");
    headers.forEach(
        (name, value) -> {
          if (json.length() > 1) {
            json.append(',');
          }
          appendString(json, name);
          json.append(':');
          appendString(json, value);
        });
    return json.append('}').toString();
  }

  private static void appendString(StringBuilder json, String value) {
    json.append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"' -> json.append("\\\"");
        case '\\' -> json.append("\\\\");
        case '\n' -> json.append("\\n");
        case '\r' -> json.append("\\r");
        case '\t' -> json.append("\\t");
        default -> {
          if (c < 0x20) {
            json.append(String.format("\\u%04x", (int) c));
          } else {
            json.append(c);
          }
        }
      }
    }
    json.append('"');
  }
}
