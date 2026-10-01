package io.github.yashrenhiet.lego.relay.outbox;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** An outbox row as read by the relay. */
public record OutboxRecord(
    long id,
    UUID eventId,
    String destination,
    String key,
    int partition,
    Map<String, String> headers,
    String payload,
    int attempts,
    Instant createdAt) {}
