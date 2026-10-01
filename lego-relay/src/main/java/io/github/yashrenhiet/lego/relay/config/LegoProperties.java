package io.github.yashrenhiet.lego.relay.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * All relay configuration, bound from {@code lego.*}.
 *
 * <p>Destinations are declared in configuration (not in a database) on purpose: they are
 * version-controlled, reviewed, and validated at startup.
 */
@Validated
@ConfigurationProperties(prefix = "lego")
public record LegoProperties(
    @DefaultValue("") String instanceId,
    @NotNull @DefaultValue("30s") Duration shutdownTimeout,
    @Valid @NotNull @DefaultValue Polling polling,
    @Valid @NotNull @DefaultValue Leasing leasing,
    @Valid @NotNull @DefaultValue Retry retry,
    @Valid @NotNull @DefaultValue Map<String, Destination> destinations) {

  /**
   * How the relay reads the outbox. {@code maxConsecutiveBatches} caps how long one partition can
   * hog a worker before yielding to other partitions.
   */
  public record Polling(
      @Min(1) @DefaultValue("100") int batchSize,
      @Min(1) @DefaultValue("8") int workerThreads,
      @NotNull @DefaultValue("200ms") Duration interval,
      @Min(1) @DefaultValue("10") int maxConsecutiveBatches) {}

  /** Partition ownership. {@code leaseDuration} is how long a crashed instance keeps its work. */
  public record Leasing(
      @NotNull @DefaultValue("30s") Duration leaseDuration,
      @NotNull @DefaultValue("5s") Duration renewInterval) {}

  /** Exponential backoff: {@code min(maxBackoff, initialBackoff * multiplier^(attempt-1))}. */
  public record Retry(
      @Min(1) @DefaultValue("10") int maxAttempts,
      @NotNull @DefaultValue("1s") Duration initialBackoff,
      @NotNull @DefaultValue("10m") Duration maxBackoff,
      @DecimalMin("1.0") @DefaultValue("2.0") double multiplier) {}

  /** Where events with a given {@code destination} name are delivered. */
  public record Destination(
      @NotNull Type type,
      @Valid @DefaultValue Kafka kafka,
      @Valid @DefaultValue Http http) {

    public enum Type {
      KAFKA,
      HTTP,
      LOG
    }
  }

  public record Kafka(String topic, @NotNull @DefaultValue("10s") Duration sendTimeout) {}

  /**
   * HTTP destination. 2xx is success; 408, 429, 5xx and I/O errors are retried; any other status
   * is a permanent failure and the event goes straight to the dead-letter state.
   */
  public record Http(
      URI url,
      @DefaultValue("POST") String method,
      @DefaultValue Map<String, String> headers,
      @NotNull @DefaultValue("5s") Duration timeout,
      @NotNull @DefaultValue("5s") Duration connectTimeout) {}
}
