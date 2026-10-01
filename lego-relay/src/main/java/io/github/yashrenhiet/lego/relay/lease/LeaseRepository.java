package io.github.yashrenhiet.lego.relay.lease;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/** SQL for instance heartbeats and partition leases. Returned maps are partition -> generation. */
@Repository
public class LeaseRepository {

  private final JdbcClient jdbc;

  public LeaseRepository(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  /** Records a heartbeat, forgets dead instances, and returns the number of live instances. */
  @Transactional
  public int heartbeat(String instanceId, Duration leaseDuration) {
    jdbc.sql(
            """
            INSERT INTO lego_instance (instance_id, heartbeat_at) VALUES (:id, now())
            ON CONFLICT (instance_id) DO UPDATE SET heartbeat_at = now()
            """)
        .param("id", instanceId)
        .update();
    jdbc.sql("DELETE FROM lego_instance WHERE heartbeat_at < now() - :ttl * interval '1 millisecond'")
        .param("ttl", leaseDuration.toMillis())
        .update();
    return jdbc.sql("SELECT count(*) FROM lego_instance").query(Integer.class).single();
  }

  public int totalPartitions() {
    return jdbc.sql("SELECT count(*) FROM lego_partition").query(Integer.class).single();
  }

  /** Extends every lease this instance still holds. Leases that already expired are not revived. */
  public Map<Integer, Long> renew(String instanceId, Duration leaseDuration) {
    return toMap(
        jdbc.sql(
                """
                UPDATE lego_partition
                   SET lease_until = now() + :ttl * interval '1 millisecond'
                 WHERE owner = :owner AND lease_until > now()
             RETURNING partition_no, generation
                """)
            .param("ttl", leaseDuration.toMillis())
            .param("owner", instanceId)
            .query(Row.class)
            .list());
  }

  /** Claims up to {@code limit} unowned or expired partitions, bumping their generation. */
  public Map<Integer, Long> claim(String instanceId, Duration leaseDuration, int limit) {
    return toMap(
        jdbc.sql(
                """
                UPDATE lego_partition
                   SET owner = :owner,
                       lease_until = now() + :ttl * interval '1 millisecond',
                       generation = generation + 1
                 WHERE partition_no IN (
                       SELECT partition_no FROM lego_partition
                        WHERE owner IS NULL OR lease_until <= now()
                        ORDER BY partition_no
                        LIMIT :limit
                          FOR UPDATE SKIP LOCKED)
             RETURNING partition_no, generation
                """)
            .param("owner", instanceId)
            .param("ttl", leaseDuration.toMillis())
            .param("limit", limit)
            .query(Row.class)
            .list());
  }

  public void release(String instanceId, List<Integer> partitions) {
    if (partitions.isEmpty()) {
      return;
    }
    jdbc.sql(
            """
            UPDATE lego_partition SET owner = NULL, lease_until = NULL
             WHERE owner = :owner AND partition_no IN (:partitions)
            """)
        .param("owner", instanceId)
        .param("partitions", partitions)
        .update();
  }

  @Transactional
  public void releaseAll(String instanceId) {
    jdbc.sql("UPDATE lego_partition SET owner = NULL, lease_until = NULL WHERE owner = :owner")
        .param("owner", instanceId)
        .update();
    jdbc.sql("DELETE FROM lego_instance WHERE instance_id = :id").param("id", instanceId).update();
  }

  private static Map<Integer, Long> toMap(List<Row> rows) {
    return rows.stream().collect(Collectors.toMap(Row::partitionNo, Row::generation));
  }

  record Row(int partitionNo, long generation) {}
}
