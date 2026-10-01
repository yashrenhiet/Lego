package io.github.yashrenhiet.lego.relay;

import io.github.yashrenhiet.lego.client.OutboxEvent;
import io.github.yashrenhiet.lego.client.OutboxWriter;
import io.github.yashrenhiet.lego.client.TestDatabase;
import io.github.yashrenhiet.lego.relay.config.LegoProperties;
import io.github.yashrenhiet.lego.relay.delivery.PartitionProcessor;
import io.github.yashrenhiet.lego.relay.delivery.RelayMetrics;
import io.github.yashrenhiet.lego.relay.delivery.RetryPolicy;
import io.github.yashrenhiet.lego.relay.delivery.Sink;
import io.github.yashrenhiet.lego.relay.delivery.SinkRegistry;
import io.github.yashrenhiet.lego.relay.lease.LeaseRepository;
import io.github.yashrenhiet.lego.relay.lease.PartitionLeaseManager;
import io.github.yashrenhiet.lego.relay.outbox.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.jdbc.core.simple.JdbcClient;

/** Builds real relay components against the embedded database, without a Spring context. */
public final class RelayFixture {

  public static final LegoProperties.Leasing LEASING =
      new LegoProperties.Leasing(Duration.ofSeconds(30), Duration.ofSeconds(5));

  public final DataSource dataSource = TestDatabase.dataSource();
  public final JdbcClient jdbc = JdbcClient.create(dataSource);
  public final LeaseRepository leaseRepository = new LeaseRepository(jdbc);
  public final OutboxRepository outbox = new OutboxRepository(jdbc, new ObjectMapper());

  public RelayFixture() {
    TestDatabase.reset();
  }

  public PartitionLeaseManager leaseManager(String instanceId) {
    return new PartitionLeaseManager(leaseRepository, instanceId, LEASING, Clock.systemUTC());
  }

  public PartitionProcessor processor(PartitionLeaseManager leases, Map<String, Sink> sinks, int maxAttempts) {
    RetryPolicy retry =
        new RetryPolicy(new LegoProperties.Retry(maxAttempts, Duration.ofMinutes(1), Duration.ofMinutes(10), 2.0));
    return new PartitionProcessor(
        outbox, leases, new SinkRegistry(sinks), retry, new RelayMetrics(new SimpleMeterRegistry()), 100);
  }

  public void write(String destination, String key, String payload) {
    try (Connection connection = dataSource.getConnection()) {
      new OutboxWriter().write(connection, OutboxEvent.builder(destination, key, payload).build());
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  public int partitionOf(String key) {
    return jdbc.sql("SELECT lego_partition_of(:key)").param("key", key).query(Integer.class).single();
  }

  public long count(String status) {
    return jdbc.sql("SELECT count(*) FROM lego_outbox WHERE status = :status")
        .param("status", status)
        .query(Long.class)
        .single();
  }

  /** Makes every pending retry due now, simulating the passage of time. */
  public void fastForwardRetries() {
    jdbc.sql("UPDATE lego_outbox SET next_attempt_at = now() WHERE status = 'PENDING'").update();
  }
}
