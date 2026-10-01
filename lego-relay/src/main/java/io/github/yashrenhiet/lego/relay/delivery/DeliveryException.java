package io.github.yashrenhiet.lego.relay.delivery;

/**
 * Thrown by a {@link Sink} when delivery fails.
 *
 * <p>A <b>permanent</b> failure (e.g. HTTP 400, oversized Kafka record) will never succeed by
 * retrying, so the event is dead-lettered immediately. Anything else is retried with backoff.
 * Sinks may also throw any {@link RuntimeException}; those are treated as retryable.
 */
public final class DeliveryException extends Exception {

  private final boolean permanent;

  private DeliveryException(String message, Throwable cause, boolean permanent) {
    super(message, cause);
    this.permanent = permanent;
  }

  public static DeliveryException retryable(String message, Throwable cause) {
    return new DeliveryException(message, cause, false);
  }

  public static DeliveryException permanent(String message, Throwable cause) {
    return new DeliveryException(message, cause, true);
  }

  public boolean isPermanent() {
    return permanent;
  }
}
