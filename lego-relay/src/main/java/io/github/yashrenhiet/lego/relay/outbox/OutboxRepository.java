package io.github.yashrenhiet.lego.relay.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.yashrenhiet.lego.relay.lease.Lease;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * All SQL the relay runs against {@code lego_outbox}.
 *
 * <p>Every state change is <b>fenced</b>: it only applies if this instance still owns the
 * partition with the same lease generation. The {@code FOR SHARE} lock on the partition row makes
 * a concurrent claim by another instance wait until the write has committed.
 */
@Repository
public class OutboxRepository {

  private static final String FENCE =
      """
      WITH fence AS (
          SELECT 1 FROM lego_partition
           WHERE partition_no = :partition AND owner = :owner AND generation = :generation
             AND lease_until > now()
             FOR SHARE)
      """;

  private static final TypeReference<Map<String, String>> HEADERS = new TypeReference<>() {};

  private final JdbcClient jdbc;
  private final ObjectMapper json;
  private final RowMapper<OutboxRecord> rowMapper = this::mapRow;

  public OutboxRepository(JdbcClient jdbc, ObjectMapper json) {
    this.jdbc = jdbc;
    this.json = json;
  }

  /**
   * The next deliverable events of a partition, oldest first.
   *
   * <p>An event is deliverable when it is due and no <em>earlier</em> pending event with the same
   * destination and key is still waiting (for a retry or a scheduled time). This is what gives
   * strict per-key ordering across polls: a failing event blocks the rest of its key, for that
   * destination only, until it succeeds or is dead-lettered.
   */
  public List<OutboxRecord> findDeliverable(int partition, int limit) {
    return jdbc.sql(
            """
            SELECT o.id, o.event_id, o.destination, o.event_key, o.partition_no, o.headers,
                   o.payload, o.attempts, o.created_at
              FROM lego_outbox o
             WHERE o.partition_no = :partition
               AND o.status = 'PENDING'
               AND o.next_attempt_at <= now()
               AND NOT EXISTS (
                   SELECT 1 FROM lego_outbox earlier
                    WHERE earlier.destination = o.destination
                      AND earlier.event_key = o.event_key
                      AND earlier.status = 'PENDING'
                      AND earlier.id < o.id
                      AND earlier.next_attempt_at > now())
             ORDER BY o.id
             LIMIT :limit
            """)
        .param("partition", partition)
        .param("limit", limit)
        .query(rowMapper)
        .list();
  }

  /** Removes delivered events. Returns how many rows were deleted (0 if the lease was lost). */
  public int deleteDelivered(Lease lease, String owner, Collection<Long> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    return fenced(
            lease, owner, "DELETE FROM lego_outbox WHERE id IN (:ids) AND EXISTS (SELECT 1 FROM fence)")
        .param("ids", ids)
        .update();
  }

  /** Records a failed attempt and schedules the next one. Returns false if the lease was lost. */
  public boolean scheduleRetry(Lease lease, String owner, long id, Duration backoff, String error) {
    return fenced(
                lease,
                owner,
                """
                UPDATE lego_outbox
                   SET attempts = attempts + 1,
                       next_attempt_at = now() + :backoff * interval '1 millisecond',
                       last_error = :error
                 WHERE id = :id AND EXISTS (SELECT 1 FROM fence)
                """)
            .param("id", id)
            .param("backoff", backoff.toMillis())
            .param("error", truncate(error))
            .update()
        == 1;
  }

  /** Moves an event to the dead-letter state. Returns false if the lease was lost. */
  public boolean markDead(Lease lease, String owner, long id, String error) {
    return fenced(
                lease,
                owner,
                """
                UPDATE lego_outbox
                   SET status = 'DEAD', attempts = attempts + 1, last_error = :error
                 WHERE id = :id AND EXISTS (SELECT 1 FROM fence)
                """)
            .param("id", id)
            .param("error", truncate(error))
            .update()
        == 1;
  }

  private JdbcClient.StatementSpec fenced(Lease lease, String owner, String statement) {
    return jdbc.sql(FENCE + statement)
        .param("partition", lease.partition())
        .param("owner", owner)
        .param("generation", lease.generation());
  }

  private OutboxRecord mapRow(ResultSet row, int rowNum) throws SQLException {
    return new OutboxRecord(
        row.getLong("id"),
        row.getObject("event_id", UUID.class),
        row.getString("destination"),
        row.getString("event_key"),
        row.getInt("partition_no"),
        parseHeaders(row.getString("headers")),
        row.getString("payload"),
        row.getInt("attempts"),
        row.getTimestamp("created_at").toInstant());
  }

  private Map<String, String> parseHeaders(String value) {
    try {
      return json.readValue(value, HEADERS);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Corrupt headers JSON in lego_outbox", e);
    }
  }

  static String truncate(String error) {
    if (error == null) {
      return null;
    }
    return error.length() <= 2000 ? error : error.substring(0, 2000);
  }
}
