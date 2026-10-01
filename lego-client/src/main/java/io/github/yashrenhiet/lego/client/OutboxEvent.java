package io.github.yashrenhiet.lego.client;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * An event to be written to the outbox.
 *
 * @param eventId      unique id; sent downstream so consumers can de-duplicate
 * @param destination  name of a destination configured in the relay
 * @param key          ordering key: events with the same key are delivered in insertion order
 * @param payload      message body
 * @param headers      forwarded as Kafka record headers / HTTP headers
 * @param deliverAfter earliest delivery time, or {@code null} for "as soon as possible"
 */
public record OutboxEvent(
    UUID eventId,
    String destination,
    String key,
    String payload,
    Map<String, String> headers,
    Instant deliverAfter) {

  public OutboxEvent {
    Objects.requireNonNull(eventId, "eventId");
    requireText(destination, "destination");
    requireText(key, "key");
    Objects.requireNonNull(payload, "payload");
    headers = Map.copyOf(headers == null ? Map.of() : headers);
  }

  public static Builder builder(String destination, String key, String payload) {
    return new Builder(destination, key, payload);
  }

  private static void requireText(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
  }

  /** Fluent builder; only destination, key and payload are required. */
  public static final class Builder {
    private final String destination;
    private final String key;
    private final String payload;
    private final Map<String, String> headers = new LinkedHashMap<>();
    private UUID eventId = UUID.randomUUID();
    private Instant deliverAfter;

    private Builder(String destination, String key, String payload) {
      this.destination = destination;
      this.key = key;
      this.payload = payload;
    }

    public Builder eventId(UUID eventId) {
      this.eventId = eventId;
      return this;
    }

    public Builder header(String name, String value) {
      headers.put(name, value);
      return this;
    }

    public Builder deliverAfter(Instant deliverAfter) {
      this.deliverAfter = deliverAfter;
      return this;
    }

    public OutboxEvent build() {
      return new OutboxEvent(eventId, destination, key, payload, headers, deliverAfter);
    }
  }
}
