package io.github.yashrenhiet.lego.relay.admin;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Queries for operating the outbox: stats and dead-letter handling. */
@Repository
public class AdminRepository {

  private final JdbcClient jdbc;

  public AdminRepository(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  public List<DestinationStats> stats() {
    return jdbc.sql(
            """
            SELECT destination,
                   count(*) FILTER (WHERE status = 'PENDING') AS pending,
                   count(*) FILTER (WHERE status = 'DEAD')    AS dead,
                   min(created_at) FILTER (WHERE status = 'PENDING') AS oldest_pending
              FROM lego_outbox
             GROUP BY destination
             ORDER BY destination
            """)
        .query(DestinationStats.class)
        .list();
  }

  public List<DeadEvent> deadEvents(String destination, long afterId, int limit) {
    return jdbc.sql(
            """
            SELECT id, event_id, destination, event_key, payload, attempts, created_at, last_error
              FROM lego_outbox
             WHERE status = 'DEAD' AND (:destination::text IS NULL OR destination = :destination)
               AND id > :afterId
             ORDER BY id
             LIMIT :limit
            """)
        .param("destination", destination)
        .param("afterId", afterId)
        .param("limit", limit)
        .query(DeadEvent.class)
        .list();
  }

  /** Puts dead events back in the queue with a fresh retry budget. Returns rows changed. */
  public int replay(List<UUID> eventIds) {
    return jdbc.sql(
            """
            UPDATE lego_outbox
               SET status = 'PENDING', attempts = 0, next_attempt_at = now(), last_error = NULL
             WHERE status = 'DEAD' AND event_id IN (:ids)
            """)
        .param("ids", eventIds)
        .update();
  }

  /** Permanently deletes dead events. Returns rows deleted. */
  public int discard(List<UUID> eventIds) {
    return jdbc.sql("DELETE FROM lego_outbox WHERE status = 'DEAD' AND event_id IN (:ids)")
        .param("ids", eventIds)
        .update();
  }

  public record DestinationStats(String destination, long pending, long dead, Instant oldestPending) {}

  public record DeadEvent(
      long id,
      UUID eventId,
      String destination,
      String eventKey,
      String payload,
      int attempts,
      Instant createdAt,
      String lastError) {}
}
