package io.github.yashrenhiet.lego.relay.lease;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.yashrenhiet.lego.relay.RelayFixture;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PartitionLeaseManagerTest {

  private RelayFixture fixture;

  @BeforeEach
  void setUp() {
    fixture = new RelayFixture();
  }

  @Test
  void singleInstanceOwnsEveryPartition() {
    PartitionLeaseManager a = fixture.leaseManager("a");

    a.tick();

    assertThat(a.activeLeases()).hasSize(64);
  }

  @Test
  void partitionsAreSplitFairlyAndNeverShared() {
    PartitionLeaseManager a = fixture.leaseManager("a");
    PartitionLeaseManager b = fixture.leaseManager("b");

    a.tick(); // a takes everything while alone
    b.tick(); // b sees 2 instances, claims nothing yet (all leased)
    a.tick(); // a releases its extras
    b.tick(); // b claims them

    assertThat(partitions(a)).hasSize(32);
    assertThat(partitions(b)).hasSize(32);
    assertThat(partitions(a)).doesNotContainAnyElementsOf(partitions(b));
  }

  @Test
  void takeoverBumpsGenerationSoTheOldLeaseIsFenced() {
    PartitionLeaseManager a = fixture.leaseManager("a");
    a.tick();
    Lease before = a.current(0).orElseThrow();

    a.releaseAll();
    PartitionLeaseManager b = fixture.leaseManager("b");
    b.tick();

    assertThat(b.current(0).orElseThrow().generation()).isGreaterThan(before.generation());
  }

  @Test
  void expiredLeasesOfACrashedInstanceAreReclaimed() {
    PartitionLeaseManager crashed = fixture.leaseManager("crashed");
    crashed.tick();
    fixture.jdbc.sql("UPDATE lego_partition SET lease_until = now() - interval '1 second'").update();
    fixture.jdbc.sql("DELETE FROM lego_instance").update();

    PartitionLeaseManager survivor = fixture.leaseManager("survivor");
    survivor.tick();

    assertThat(survivor.activeLeases()).hasSize(64);
  }

  private static Set<Integer> partitions(PartitionLeaseManager manager) {
    return manager.activeLeases().stream().map(Lease::partition).collect(Collectors.toCollection(HashSet::new));
  }
}
