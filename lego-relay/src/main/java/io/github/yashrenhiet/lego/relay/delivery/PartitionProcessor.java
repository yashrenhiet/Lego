package io.github.yashrenhiet.lego.relay.delivery;

import io.github.yashrenhiet.lego.relay.lease.Lease;
import io.github.yashrenhiet.lego.relay.lease.PartitionLeaseManager;
import io.github.yashrenhiet.lego.relay.outbox.OutboxRecord;
import io.github.yashrenhiet.lego.relay.outbox.OutboxRepository;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delivers one batch of one partition.
 *
 * <p>Stateless and thread-safe; the caller guarantees a given partition is never processed by
 * two threads at once, so events of a partition are handled strictly in {@code id} order.
 */
public class PartitionProcessor {

  private static final Logger log = LoggerFactory.getLogger(PartitionProcessor.class);

  private final OutboxRepository outbox;
  private final PartitionLeaseManager leases;
  private final SinkRegistry sinks;
  private final RetryPolicy retryPolicy;
  private final RelayMetrics metrics;
  private final int batchSize;

  public PartitionProcessor(
      OutboxRepository outbox,
      PartitionLeaseManager leases,
      SinkRegistry sinks,
      RetryPolicy retryPolicy,
      RelayMetrics metrics,
      int batchSize) {
    this.outbox = outbox;
    this.leases = leases;
    this.sinks = sinks;
    this.retryPolicy = retryPolicy;
    this.metrics = metrics;
    this.batchSize = batchSize;
  }

  /**
   * Processes the next batch for {@code lease}.
   *
   * @return true if the batch was full, i.e. more work is probably waiting
   */
  public boolean processBatch(Lease lease) {
    List<OutboxRecord> batch = outbox.findDeliverable(lease.partition(), batchSize);
    List<Long> delivered = new ArrayList<>(batch.size());
    Set<String> blockedKeys = new HashSet<>(); // keys with an event rescheduled in this batch

    for (OutboxRecord event : batch) {
      if (!leases.stillHolds(lease)) {
        log.warn("Lost lease on partition {} mid-batch; stopping", lease.partition());
        break;
      }
      if (blockedKeys.contains(event.key())) {
        continue; // an earlier event of this key is waiting to retry
      }
      Outcome outcome = deliver(lease, event);
      switch (outcome) {
        case DELIVERED -> delivered.add(event.id());
        case RETRY_SCHEDULED -> blockedKeys.add(event.key());
        case DEAD_LETTERED -> { } // later events of the key may proceed
        case LEASE_LOST -> {
          flush(lease, delivered);
          return false;
        }
      }
    }
    flush(lease, delivered);
    return batch.size() == batchSize;
  }

  private Outcome deliver(Lease lease, OutboxRecord event) {
    long start = System.nanoTime();
    try {
      sinks.forDestination(event.destination()).send(event);
      metrics.delivered(event, System.nanoTime() - start);
      return Outcome.DELIVERED;
    } catch (DeliveryException e) {
      return recordFailure(lease, event, e, e.isPermanent());
    } catch (RuntimeException e) {
      return recordFailure(lease, event, e, false);
    }
  }

  private Outcome recordFailure(Lease lease, OutboxRecord event, Exception error, boolean permanent) {
    int attempts = event.attempts() + 1;
    RetryPolicy.Decision decision = retryPolicy.afterFailure(attempts, permanent);
    String message = describe(error);
    String owner = leases.instanceId();

    if (decision.isDeadLetter()) {
      log.error(
          "Dead-lettering event {} for '{}' after {} attempt(s): {}",
          event.eventId(), event.destination(), attempts, message);
      if (!outbox.markDead(lease, owner, event.id(), message)) {
        return Outcome.LEASE_LOST;
      }
      metrics.deadLettered(event);
      return Outcome.DEAD_LETTERED;
    }

    log.warn(
        "Delivery of event {} to '{}' failed (attempt {}), retrying in {}: {}",
        event.eventId(), event.destination(), attempts, decision.backoff(), message);
    if (!outbox.scheduleRetry(lease, owner, event.id(), decision.backoff(), message)) {
      return Outcome.LEASE_LOST;
    }
    metrics.retried(event);
    return Outcome.RETRY_SCHEDULED;
  }

  private void flush(Lease lease, List<Long> delivered) {
    int deleted = outbox.deleteDelivered(lease, leases.instanceId(), delivered);
    if (deleted != delivered.size()) {
      log.warn(
          "Partition {}: delivered {} events but deleted {} (lease lost); they will be redelivered",
          lease.partition(), delivered.size(), deleted);
    }
  }

  static String describe(Throwable error) {
    Throwable root = error;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    String message = error.getClass().getSimpleName() + ": " + error.getMessage();
    return root == error ? message : message + " (root cause: " + root + ")";
  }

  private enum Outcome {
    DELIVERED,
    RETRY_SCHEDULED,
    DEAD_LETTERED,
    LEASE_LOST
  }
}
