package io.github.yashrenhiet.lego.relay.delivery;

import io.github.yashrenhiet.lego.relay.outbox.OutboxRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/** Micrometer metrics, tagged by destination. Exposed at {@code /actuator/prometheus}. */
public class RelayMetrics {

  private final MeterRegistry registry;

  public RelayMetrics(MeterRegistry registry) {
    this.registry = registry;
  }

  public void delivered(OutboxRecord event, long sendNanos) {
    counter("lego.events.delivered", event).increment();
    Timer.builder("lego.delivery.send")
        .description("Time spent in the sink")
        .tag("destination", event.destination())
        .register(registry)
        .record(sendNanos, TimeUnit.NANOSECONDS);
    Timer.builder("lego.delivery.latency")
        .description("Time from outbox insert to successful delivery")
        .tag("destination", event.destination())
        .register(registry)
        .record(Duration.between(event.createdAt(), Instant.now()));
  }

  public void retried(OutboxRecord event) {
    counter("lego.events.retried", event).increment();
  }

  public void deadLettered(OutboxRecord event) {
    counter("lego.events.dead", event).increment();
  }

  private Counter counter(String name, OutboxRecord event) {
    return Counter.builder(name).tag("destination", event.destination()).register(registry);
  }
}
