package io.github.yashrenhiet.lego.relay.sink;

import io.github.yashrenhiet.lego.relay.delivery.Sink;
import io.github.yashrenhiet.lego.relay.outbox.OutboxRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logs events instead of sending them. Handy for demos and local development. */
public class LogSink implements Sink {

  private static final Logger log = LoggerFactory.getLogger(LogSink.class);

  private final String destination;

  public LogSink(String destination) {
    this.destination = destination;
  }

  @Override
  public void send(OutboxRecord event) {
    log.info("[{}] key={} id={} payload={}", destination, event.key(), event.eventId(), event.payload());
  }
}
