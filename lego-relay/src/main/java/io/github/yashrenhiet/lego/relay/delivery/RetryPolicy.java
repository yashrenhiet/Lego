package io.github.yashrenhiet.lego.relay.delivery;

import io.github.yashrenhiet.lego.relay.config.LegoProperties;
import java.time.Duration;

/**
 * Decides what happens after a failed delivery attempt.
 *
 * <p>Backoff is exponential and capped: {@code min(maxBackoff, initial * multiplier^(attempt-1))}.
 */
public final class RetryPolicy {

  private final int maxAttempts;
  private final long initialMillis;
  private final long maxMillis;
  private final double multiplier;

  public RetryPolicy(LegoProperties.Retry config) {
    this.maxAttempts = config.maxAttempts();
    this.initialMillis = config.initialBackoff().toMillis();
    this.maxMillis = config.maxBackoff().toMillis();
    this.multiplier = config.multiplier();
  }

  /** The decision for an event that has now failed {@code attempts} times in total. */
  public Decision afterFailure(int attempts, boolean permanent) {
    if (permanent || attempts >= maxAttempts) {
      return Decision.deadLetter();
    }
    double delay = initialMillis * Math.pow(multiplier, attempts - 1);
    return Decision.retryIn(Duration.ofMillis((long) Math.min(maxMillis, delay)));
  }

  /** Either "retry after {@code backoff}" or "dead-letter" ({@code backoff == null}). */
  public record Decision(Duration backoff) {

    static Decision retryIn(Duration backoff) {
      return new Decision(backoff);
    }

    static Decision deadLetter() {
      return new Decision(null);
    }

    public boolean isDeadLetter() {
      return backoff == null;
    }
  }
}
