# Lego

[![build](https://github.com/yashrenhiet/Lego/actions/workflows/build.yml/badge.svg)](https://github.com/yashrenhiet/Lego/actions/workflows/build.yml)

A small, dependable **transactional outbox relay** for PostgreSQL. Write an event in the same
transaction as your business data; Lego delivers it to Kafka or HTTP **at least once**, **in order
per key**, with retries, a dead-letter state and a replay API.

```
 your service                      PostgreSQL                         Lego relay (N instances)
┌──────────────┐  one transaction ┌──────────────┐  poll own partitions ┌──────────────┐   ┌────────┐
│ UPDATE order │ ───────────────► │ orders       │                      │ lease 64     │──►│ Kafka  │
│ INSERT event │                  │ lego_outbox  │ ◄──────────────────► │ partitions   │──►│ HTTP   │
└──────────────┘                  └──────────────┘  delete on success   └──────────────┘   └────────┘
```

## Why

"Save to the database, then publish to Kafka" is two writes. If the process dies between them,
you either lose the event or publish one that never happened. The outbox pattern fixes this by
making the event part of the database transaction and letting a separate relay publish it.

## Features

- **Atomic writes**: a tiny zero-dependency client (`lego-client`) inserts events using *your*
  JDBC connection. Or use a plain `INSERT`; the partition is computed by the database.
- **Per-key ordering**: events with the same destination and key are delivered in insertion
  order, even across failures. A failing event blocks only its own key on its own destination;
  everything else keeps flowing.
- **Horizontal scaling with no leader**: 64 partitions are leased by relay instances; each takes
  its fair share. Add or remove instances at any time.
- **Fencing**: every write is guarded by the lease *generation*, so a paused or partitioned
  instance can never act on a partition it lost.
- **Retries and dead letters**: exponential backoff; permanent errors (HTTP 4xx, oversized
  records) and exhausted retries move the event to `DEAD`. Replay or discard via the admin API.
- **Sinks**: Kafka (idempotent producer, ack required), HTTP (JDK client), and a log sink. Adding
  one is a single interface.
- **Observability**: Prometheus metrics for delivered / retried / dead events and end-to-end latency.

## Quick start

```bash
docker compose up --build        # PostgreSQL, Kafka and two relay instances
```

Write an event (any language works; this is just SQL):

```sql
INSERT INTO lego_outbox (destination, event_key, payload)
VALUES ('orders', 'order-42', '{"status":"PAID"}');
```

It shows up on the `orders.v1` topic, keyed by `order-42`.

### From Java

```xml
<dependency>
  <groupId>io.github.yashrenhiet</groupId>
  <artifactId>lego-client</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```java
OutboxWriter outbox = new OutboxWriter();

try (Connection tx = dataSource.getConnection()) {
  tx.setAutoCommit(false);
  orderDao.markPaid(tx, orderId);
  outbox.write(tx, OutboxEvent.builder("orders", orderId, json).header("type", "OrderPaid").build());
  tx.commit(); // both or neither
}
```

With Spring's `@Transactional`, pass `DataSourceUtils.getConnection(dataSource)`.

The schema ships inside `lego-client` as a Flyway migration at `classpath:db/lego`. Add that
location to your own Flyway config, or let the relay apply it.

## Configuration

Destinations live in configuration, so they are version-controlled and validated at startup.

```yaml
lego:
  destinations:
    orders:
      type: KAFKA
      kafka:
        topic: orders.v1
        send-timeout: 10s    # keep >= producer max.block.ms + delivery.timeout.ms
    billing-webhook:
      type: HTTP
      http:
        url: https://billing.example.com/hooks
        timeout: 5s
        connect-timeout: 5s
        headers:
          Authorization: Bearer ${BILLING_TOKEN}
  retry:
    max-attempts: 10       # then DEAD
    initial-backoff: 1s    # 1s, 2s, 4s, ... capped at max-backoff
    max-backoff: 10m
  polling:
    batch-size: 100
    worker-threads: 8
    interval: 200ms
    max-consecutive-batches: 10  # fairness: batches per partition before yielding
  leasing:
    lease-duration: 30s    # how long a crashed instance's partitions stay blocked
    renew-interval: 5s
  shutdown-timeout: 30s    # keep > your slowest sink timeout
  admin:
    write-enabled: false   # enable replay/discard endpoints
```

| HTTP response | Result |
|---|---|
| 2xx | delivered |
| 408, 429, 5xx, timeout, connection error | retried with backoff |
| any other 4xx | dead-lettered immediately |

## Admin API

| Endpoint | |
|---|---|
| `GET /admin/stats` | pending / dead counts and oldest pending event per destination |
| `GET /admin/dead-events?destination=&after=&limit=` | browse dead letters (keyset pagination) |
| `POST /admin/dead-events/replay` `{"eventIds":[...]}` | re-queue with a fresh retry budget * |
| `POST /admin/dead-events/discard` `{"eventIds":[...]}` | delete permanently * |
| `GET :8081/actuator/prometheus` | metrics (separate management port, `MANAGEMENT_PORT`) |

\* Data-changing endpoints are **off by default**; enable them with
`lego.admin.write-enabled: true`. There is no built-in authentication, so only enable them behind
your gateway or on an internal network.

## Guarantees

- **At least once.** A crash between publish and delete causes a redelivery. Consumers should
  de-duplicate on `lego-event-id` (Kafka header) / `Lego-Event-Id` (HTTP header).
- **Ordered per (destination, key).** There is no ordering across keys or destinations.
- **Latency** is roughly the poll interval (200 ms by default) when the system is healthy. After
  a crash, that instance's partitions resume once its lease expires.

See [docs/DESIGN.md](docs/DESIGN.md) for how it works and the trade-offs.

## Building

```bash
mvn verify   # Java 21; tests use an embedded PostgreSQL, no Docker needed
```

## License

[Apache 2.0](LICENSE)
