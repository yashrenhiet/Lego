package io.github.yashrenhiet.lego.relay.delivery;

import java.util.Map;

/** Maps destination names to sinks. Unknown destinations fail permanently (dead-letter). */
public class SinkRegistry {

  private final Map<String, Sink> sinks;

  public SinkRegistry(Map<String, Sink> sinks) {
    this.sinks = Map.copyOf(sinks);
  }

  public Sink forDestination(String destination) {
    Sink sink = sinks.get(destination);
    if (sink != null) {
      return sink;
    }
    return event -> {
      throw DeliveryException.permanent(
          "No destination named '" + destination + "' is configured under lego.destinations", null);
    };
  }
}
