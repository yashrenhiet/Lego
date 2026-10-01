package io.github.yashrenhiet.lego.relay.lease;

import io.github.yashrenhiet.lego.relay.config.LegoProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps this instance's fair share of partitions leased. There is no leader: every instance
 * independently renews what it has, gives back extras, and claims free partitions.
 *
 * <pre>
 *   fairShare = ceil(totalPartitions / liveInstances)
 *   held > fairShare  -> release the extras (another instance will claim them)
 *   held < fairShare  -> claim unowned / expired partitions
 * </pre>
 *
 * A crashed instance's partitions become claimable once its lease expires. A clean shutdown
 * releases them immediately.
 */
public class PartitionLeaseManager {

  private static final Logger log = LoggerFactory.getLogger(PartitionLeaseManager.class);

  private final LeaseRepository repository;
  private final String instanceId;
  private final Duration leaseDuration;
  private final Duration safetyMargin;
  private final Clock clock;
  private final Map<Integer, Lease> leases = new ConcurrentHashMap<>();

  public PartitionLeaseManager(
      LeaseRepository repository, String instanceId, LegoProperties.Leasing config, Clock clock) {
    if (config.renewInterval().multipliedBy(2).compareTo(config.leaseDuration()) > 0) {
      throw new IllegalArgumentException("lego.leasing.renew-interval must be <= lease-duration / 2");
    }
    this.repository = repository;
    this.instanceId = instanceId;
    this.leaseDuration = config.leaseDuration();
    this.safetyMargin = config.renewInterval();
    this.clock = clock;
  }

  /** Runs one heartbeat / renew / rebalance round. Called periodically. */
  public synchronized void tick() {
    Instant requestedAt = clock.instant();
    int liveInstances = repository.heartbeat(instanceId, leaseDuration);
    int fairShare = ceilDiv(repository.totalPartitions(), Math.max(1, liveInstances));

    Map<Integer, Long> held = repository.renew(instanceId, leaseDuration);
    if (held.size() > fairShare) {
      List<Integer> extras = new ArrayList<>(held.keySet()).subList(fairShare, held.size());
      repository.release(instanceId, extras);
      extras.forEach(held::remove);
      log.info("Released partitions {} to rebalance ({} live instances)", extras, liveInstances);
    } else if (held.size() < fairShare) {
      Map<Integer, Long> claimed = repository.claim(instanceId, leaseDuration, fairShare - held.size());
      if (!claimed.isEmpty()) {
        log.info("Claimed partitions {} ({} live instances)", claimed.keySet(), liveInstances);
      }
      held.putAll(claimed);
    }
    replaceLeases(held, requestedAt.plus(leaseDuration).minus(safetyMargin));
  }

  /** The current lease for a partition, if this instance holds a still-valid one. */
  public Optional<Lease> current(int partition) {
    return Optional.ofNullable(leases.get(partition)).filter(lease -> lease.isValid(clock));
  }

  /** True while {@code lease} is still the lease this instance holds for its partition. */
  public boolean stillHolds(Lease lease) {
    return current(lease.partition()).map(l -> l.generation() == lease.generation()).orElse(false);
  }

  public Collection<Lease> activeLeases() {
    return leases.values().stream().filter(lease -> lease.isValid(clock)).toList();
  }

  public String instanceId() {
    return instanceId;
  }

  /** Gives every partition back so other instances can take over without waiting for expiry. */
  public synchronized void releaseAll() {
    leases.clear();
    repository.releaseAll(instanceId);
    log.info("Released all partitions for shutdown");
  }

  private void replaceLeases(Map<Integer, Long> held, Instant validUntil) {
    leases.keySet().retainAll(held.keySet());
    held.forEach((partition, generation) -> leases.put(partition, new Lease(partition, generation, validUntil)));
  }

  private static int ceilDiv(int dividend, int divisor) {
    return (dividend + divisor - 1) / divisor;
  }
}
