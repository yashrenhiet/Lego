package io.github.yashrenhiet.lego.relay.lease;

import java.time.Clock;
import java.time.Instant;

/**
 * Proof that this instance owns a partition.
 *
 * <p>{@code generation} is the fencing token: every write the relay makes on behalf of a
 * partition is guarded by {@code owner = me AND generation = lease.generation}, so a paused or
 * partitioned instance that lost its lease can never corrupt the new owner's state.
 *
 * @param validUntil local-clock deadline, deliberately shorter than the database lease
 */
public record Lease(int partition, long generation, Instant validUntil) {

  public boolean isValid(Clock clock) {
    return clock.instant().isBefore(validUntil);
  }
}
