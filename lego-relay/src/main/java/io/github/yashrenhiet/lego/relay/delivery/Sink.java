package io.github.yashrenhiet.lego.relay.delivery;

import io.github.yashrenhiet.lego.relay.outbox.OutboxRecord;

/** Delivers an event to an external system. Implementations must be thread-safe. */
public interface Sink {

  /**
   * Delivers the event, returning only once the downstream system has acknowledged it.
   *
   * @throws DeliveryException if delivery failed; see {@link DeliveryException#isPermanent()}
   */
  void send(OutboxRecord event) throws DeliveryException;
}
