package io.github.yashrenhiet.lego.relay.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.yashrenhiet.lego.relay.RelayFixture;
import io.github.yashrenhiet.lego.relay.lease.Lease;
import io.github.yashrenhiet.lego.relay.lease.PartitionLeaseManager;
import io.github.yashrenhiet.lego.relay.outbox.OutboxRecord;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PartitionProcessorTest {

  private RelayFixture fixture;
  private PartitionLeaseManager leases;
  private RecordingSink sink;

  @BeforeEach
  void setUp() {
    fixture = new RelayFixture();
    leases = fixture.leaseManager("relay-a");
    leases.tick(); // single instance: owns every partition
    sink = new RecordingSink();
  }

  @Test
  void deliversInInsertionOrderAndDeletesDeliveredEvents() {
    for (int i = 1; i <= 5; i++) {
      fixture.write("orders", "order-1", "e" + i);
    }

    process(3, "order-1");

    assertThat(sink.payloads).containsExactly("e1", "e2", "e3", "e4", "e5");
    assertThat(fixture.count("PENDING")).isZero();
  }

  @Test
  void failedEventBlocksLaterEventsOfSameKeyButNotOtherKeys() {
    String blockedKey = "order-1";
    String otherKey = keyInSamePartitionAs(blockedKey);
    fixture.write("orders", blockedKey, "a1");
    fixture.write("orders", otherKey, "b1");
    fixture.write("orders", blockedKey, "a2");
    sink.failTimes("a1", 1, false);

    processPartitionOf(blockedKey);
    assertThat(sink.payloads).containsExactly("b1");

    // Next poll: a1 is still in backoff, so a2 must keep waiting.
    processPartitionOf(blockedKey);
    assertThat(sink.payloads).containsExactly("b1");

    fixture.fastForwardRetries();
    processPartitionOf(blockedKey);
    assertThat(sink.payloads).containsExactly("b1", "a1", "a2");
    assertThat(fixture.count("PENDING")).isZero();
  }

  @Test
  void permanentFailureDeadLettersAndUnblocksTheKey() {
    fixture.write("orders", "k", "poison");
    fixture.write("orders", "k", "next");
    sink.failTimes("poison", Integer.MAX_VALUE, true);

    processPartitionOf("k");

    assertThat(sink.payloads).containsExactly("next");
    assertThat(fixture.count("DEAD")).isEqualTo(1);
    assertThat(fixture.count("PENDING")).isZero();
  }

  @Test
  void retryableFailureDeadLettersAfterMaxAttempts() {
    fixture.write("orders", "k", "flaky");
    sink.failTimes("flaky", Integer.MAX_VALUE, false);

    for (int attempt = 0; attempt < 3; attempt++) {
      processPartitionOf("k");
      fixture.fastForwardRetries();
    }

    assertThat(fixture.count("DEAD")).isEqualTo(1);
    String lastError =
        fixture.jdbc.sql("SELECT last_error FROM lego_outbox").query(String.class).single();
    assertThat(lastError).contains("boom");
  }

  @Test
  void failureOnOneDestinationDoesNotBlockSameKeyOnAnother() {
    RecordingSink webhook = new RecordingSink();
    fixture.write("orders", "customer-1", "o1");
    fixture.write("webhook", "customer-1", "w1");
    fixture.write("orders", "customer-1", "o2");
    fixture.write("webhook", "customer-1", "w2");
    sink.failTimes("o1", 1, false);

    processWith(Map.of("orders", sink, "webhook", webhook), "customer-1");
    processWith(Map.of("orders", sink, "webhook", webhook), "customer-1");

    assertThat(webhook.payloads).containsExactly("w1", "w2");
    assertThat(sink.payloads).isEmpty(); // o2 still waits behind o1
  }

  @Test
  void unknownDestinationIsDeadLettered() {
    fixture.write("nowhere", "k", "p");

    processPartitionOf("k");

    assertThat(fixture.count("DEAD")).isEqualTo(1);
  }

  @Test
  void writesWithAStaleLeaseAreRejected() {
    fixture.write("orders", "k", "p");
    int partition = fixture.partitionOf("k");
    Lease stale = leases.current(partition).orElseThrow();
    // Another instance takes over the partition (generation is bumped).
    fixture.jdbc.sql("UPDATE lego_partition SET owner = 'relay-b', generation = generation + 1 WHERE partition_no = :p")
        .param("p", partition)
        .update();

    List<OutboxRecord> events = fixture.outbox.findDeliverable(partition, 10);
    int deleted = fixture.outbox.deleteDelivered(stale, "relay-a", List.of(events.getFirst().id()));

    assertThat(deleted).isZero();
    assertThat(fixture.count("PENDING")).isEqualTo(1);
  }

  private void process(int maxAttempts, String key) {
    fixture.processor(leases, Map.of("orders", sink), maxAttempts)
        .processBatch(leases.current(fixture.partitionOf(key)).orElseThrow());
  }

  private void processWith(Map<String, Sink> sinks, String key) {
    fixture.processor(leases, sinks, 3)
        .processBatch(leases.current(fixture.partitionOf(key)).orElseThrow());
  }

  private void processPartitionOf(String key) {
    process(3, key);
  }

  private String keyInSamePartitionAs(String key) {
    int partition = fixture.partitionOf(key);
    for (int i = 0; ; i++) {
      String candidate = "other-" + i;
      if (fixture.partitionOf(candidate) == partition) {
        return candidate;
      }
    }
  }

  /** Records delivered payloads; can be told to fail specific payloads. */
  static final class RecordingSink implements Sink {
    final List<String> payloads = new ArrayList<>();
    private final Map<String, Integer> failuresLeft = new java.util.HashMap<>();
    private final Set<String> permanent = new HashSet<>();

    void failTimes(String payload, int times, boolean isPermanent) {
      failuresLeft.put(payload, times);
      if (isPermanent) {
        permanent.add(payload);
      }
    }

    @Override
    public void send(OutboxRecord event) throws DeliveryException {
      int left = failuresLeft.getOrDefault(event.payload(), 0);
      if (left > 0) {
        failuresLeft.put(event.payload(), left - 1);
        throw permanent.contains(event.payload())
            ? DeliveryException.permanent("boom (permanent)", null)
            : DeliveryException.retryable("boom", null);
      }
      payloads.add(event.payload());
    }
  }
}
