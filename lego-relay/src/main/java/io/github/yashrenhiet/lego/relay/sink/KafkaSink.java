package io.github.yashrenhiet.lego.relay.sink;

import io.github.yashrenhiet.lego.relay.config.LegoProperties;
import io.github.yashrenhiet.lego.relay.delivery.DeliveryException;
import io.github.yashrenhiet.lego.relay.delivery.Sink;
import io.github.yashrenhiet.lego.relay.outbox.OutboxRecord;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Publishes to a Kafka topic and waits for the broker ack (bounded by {@code send-timeout}).
 *
 * <p>The record key is the event key, so Kafka keeps the same per-key ordering. The event id is
 * sent as the {@code lego-event-id} header for consumer-side de-duplication.
 */
public class KafkaSink implements Sink {

  static final String EVENT_ID_HEADER = "lego-event-id";

  private final KafkaTemplate<String, String> kafka;
  private final String topic;
  private final Duration sendTimeout;

  public KafkaSink(KafkaTemplate<String, String> kafka, LegoProperties.Kafka config) {
    if (config.topic() == null || config.topic().isBlank()) {
      throw new IllegalArgumentException("Kafka destination requires 'kafka.topic'");
    }
    this.kafka = kafka;
    this.topic = config.topic();
    this.sendTimeout = config.sendTimeout();
  }

  @Override
  public void send(OutboxRecord event) throws DeliveryException {
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.key(), event.payload());
    record.headers().add(EVENT_ID_HEADER, event.eventId().toString().getBytes(StandardCharsets.UTF_8));
    event.headers().forEach((name, value) ->
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8)));
    try {
      kafka.send(record).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw DeliveryException.retryable("Interrupted while sending to Kafka", e);
    } catch (TimeoutException e) {
      throw DeliveryException.retryable("Kafka did not ack within " + sendTimeout, e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RecordTooLargeException || cause instanceof SerializationException) {
        throw DeliveryException.permanent("Kafka rejected the record: " + cause.getMessage(), cause);
      }
      throw DeliveryException.retryable("Kafka send failed: " + cause.getMessage(), cause);
    }
  }
}
