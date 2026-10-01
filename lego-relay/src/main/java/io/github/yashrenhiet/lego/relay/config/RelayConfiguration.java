package io.github.yashrenhiet.lego.relay.config;

import io.github.yashrenhiet.lego.relay.delivery.PartitionProcessor;
import io.github.yashrenhiet.lego.relay.delivery.RelayEngine;
import io.github.yashrenhiet.lego.relay.delivery.RelayMetrics;
import io.github.yashrenhiet.lego.relay.delivery.RetryPolicy;
import io.github.yashrenhiet.lego.relay.delivery.Sink;
import io.github.yashrenhiet.lego.relay.delivery.SinkRegistry;
import io.github.yashrenhiet.lego.relay.lease.LeaseRepository;
import io.github.yashrenhiet.lego.relay.lease.PartitionLeaseManager;
import io.github.yashrenhiet.lego.relay.outbox.OutboxRepository;
import io.github.yashrenhiet.lego.relay.sink.HttpSink;
import io.github.yashrenhiet.lego.relay.sink.KafkaSink;
import io.github.yashrenhiet.lego.relay.sink.LogSink;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

/** Wires the relay from {@link LegoProperties}. */
@Configuration(proxyBeanMethods = false)
public class RelayConfiguration {

  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  PartitionLeaseManager partitionLeaseManager(LeaseRepository repository, LegoProperties props, Clock clock) {
    return new PartitionLeaseManager(repository, instanceId(props), props.leasing(), clock);
  }

  @Bean
  SinkRegistry sinkRegistry(LegoProperties props, ObjectProvider<KafkaTemplate<String, String>> kafka) {
    Map<String, Sink> sinks = new HashMap<>();
    props.destinations().forEach((name, destination) ->
        sinks.put(name, switch (destination.type()) {
          case KAFKA -> new KafkaSink(kafka.getObject(), destination.kafka());
          case HTTP -> new HttpSink(httpClient(destination.http()), destination.http());
          case LOG -> new LogSink(name);
        }));
    return new SinkRegistry(sinks);
  }

  @Bean
  PartitionProcessor partitionProcessor(
      OutboxRepository outbox,
      PartitionLeaseManager leases,
      SinkRegistry sinks,
      LegoProperties props,
      MeterRegistry meterRegistry) {
    return new PartitionProcessor(
        outbox,
        leases,
        sinks,
        new RetryPolicy(props.retry()),
        new RelayMetrics(meterRegistry),
        props.polling().batchSize());
  }

  @Bean
  RelayEngine relayEngine(PartitionLeaseManager leases, PartitionProcessor processor, LegoProperties props) {
    return new RelayEngine(
        leases,
        processor,
        props.polling().interval(),
        props.leasing().renewInterval(),
        props.polling().workerThreads(),
        props.polling().maxConsecutiveBatches(),
        props.shutdownTimeout());
  }

  private static HttpClient httpClient(LegoProperties.Http config) {
    return HttpClient.newBuilder().connectTimeout(config.connectTimeout()).build();
  }

  private static String instanceId(LegoProperties props) {
    if (!props.instanceId().isBlank()) {
      return props.instanceId();
    }
    String suffix = UUID.randomUUID().toString().substring(0, 8);
    try {
      return InetAddress.getLocalHost().getHostName() + "-" + suffix;
    } catch (UnknownHostException e) {
      return "relay-" + suffix;
    }
  }
}
