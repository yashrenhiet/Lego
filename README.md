<h1 align="center">Lego</h1>

<p align="center">
  <b>A small, dependable transactional-outbox relay for PostgreSQL.</b><br>
  Commit your data and your events together. Lego delivers the events to Kafka or HTTP
  at least once, in order per key, with retries and dead letters.
</p>

<p align="center">
  <a href="https://github.com/yashrenhiet/Lego/actions/workflows/build.yml"><img src="https://github.com/yashrenhiet/Lego/actions/workflows/build.yml/badge.svg" alt="Build status"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License: Apache 2.0"></a>
  <img src="https://img.shields.io/badge/java-21-orange.svg" alt="Java 21">
  <img src="https://img.shields.io/badge/PostgreSQL-13%2B-336791.svg" alt="PostgreSQL 13+">
  <img src="https://img.shields.io/badge/status-pre--1.0-yellow.svg" alt="Status: pre-1.0">
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> •
  <a href="#usage">Usage</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#guarantees">Guarantees</a> •
  <a href="docs/DESIGN.md">Design</a> •
  <a href="#faq">FAQ</a> •
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

---

## Table of contents

- [The problem](#the-problem)
- [How Lego solves it](#how-lego-solves-it)
- [Features](#features)
- [Quick start](#quick-start)
- [Usage](#usage)
- [Configuration](#configuration)
- [Guarantees](#guarantees)
- [Operations](#operations)
- [How it works](#how-it-works)
- [When to use Lego (and when not)](#when-to-use-lego-and-when-not)
- [FAQ](#faq)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

## The problem

Most services eventually need to "save something **and** tell the world about it":

```java
orderRepository.save(order);            // 1. database commit
kafka.send("orders", order.toEvent());  // 2. network call
```

These are two separate writes, and there is no transaction spanning both:

- if the process crashes **after 1 and before 2**, the order exists but the event is **lost**;
- if you flip the order, a crash after 2 publishes an event for an order that **never existed**.

Retries don't fix this, and neither does wrapping it all in `@Transactional`. This is the
classic *dual-write problem*.

## How Lego solves it

Lego implements the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html).
Your service writes the event into an outbox table **in the same database transaction** as the
business change. Lego then reads the outbox and delivers each event, deleting it once the
destination has acknowledged it.

```
  your service                 PostgreSQL                     Lego relay (1..N instances)
┌───────────────┐  one tx   ┌──────────────┐  lease, poll   ┌───────────────────┐    ┌───────┐
│ UPDATE orders │ ────────► │ orders       │ ─────────────► │ partitions 0..63  │──► │ Kafka │
│ INSERT event  │           │ lego_outbox  │ ◄───────────── │ retry / dead-     │──► │ HTTP  │
└───────────────┘           └──────────────┘ delete on ack  │ letter / metrics  │    └───────┘
                                                            └───────────────────┘
```

Either both rows commit or neither does, so an event can never be lost or invented.

## Features

- **Atomic by construction.** Events are written on *your* JDBC connection by a tiny,
  zero-dependency client, or with a plain SQL `INSERT` from any language.
- **Ordered per key.** Events with the same destination and key are delivered in insertion
  order, even across retries, restarts and instances. A failing event holds back only its own
  key; everything else keeps flowing.
- **Scales out with no leader.** 64 partitions are leased by however many relay instances you
  run, and each takes its fair share. Instances can be added or removed at any time.
- **Fenced writes.** Every write carries the lease *generation*, so a paused or partitioned
  instance can't touch a partition it no longer owns.
- **Retries and dead letters.** Failures retry with capped exponential backoff. Permanent errors
  (HTTP 4xx, oversized Kafka records) and exhausted retries move the event to a `DEAD` state that
  you can inspect, replay or discard.
- **Kafka and HTTP built in.** Kafka uses an idempotent producer with `acks=all`; HTTP uses the
  JDK client. A log sink is included for local work, and adding a sink means implementing one interface.
- **Observable.** Prometheus metrics for delivered, retried and dead events and end-to-end
  latency, plus health probes.
- **Small.** Roughly 1,500 lines of Java and a 60-line schema.

## Quick start

**Prerequisites:** Docker with Compose v2.

```bash
git clone https://github.com/yashrenhiet/Lego.git
cd Lego
docker compose up --build
```

That starts PostgreSQL, Kafka and **two** relay instances sharing the work. In another terminal,
insert an event using nothing but SQL:

```bash
docker compose exec postgres psql -U lego -d lego -c \
  "INSERT INTO lego_outbox (destination, event_key, payload)
   VALUES ('orders', 'order-42', '{\"status\":\"PAID\"}');"
```

Then watch it arrive on Kafka, keyed by `order-42`:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic orders.v1 --from-beginning --property print.key=true
```

```text
order-42	{"status":"PAID"}
```

Check the outbox (an empty list means everything was delivered):

```bash
curl -s localhost:8081/admin/stats   # []
```

> The whole sequence above is automated in [`scripts/smoke-test.sh`](scripts/smoke-test.sh) and
> runs in CI on every push.

## Usage

### 1. Create the schema

The schema ships inside the `lego-client` jar as a Flyway migration at `classpath:db/lego`.
Either let the relay apply it on startup (the default), or add the location to your service's
own Flyway configuration:

```yaml
spring:
  flyway:
    locations: classpath:db/migration, classpath:db/lego
```

Not using Flyway? Apply
[`V1__outbox.sql`](lego-client/src/main/resources/db/lego/V1__outbox.sql) with your usual
migration tool.

### 2. Write events in your transaction

#### Java

> `lego-client` is not on Maven Central yet. Until it is, build it locally with `mvn install`.

```xml
<dependency>
  <groupId>io.github.yashrenhiet</groupId>
  <artifactId>lego-client</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```java
OutboxWriter outbox = new OutboxWriter(); // stateless and thread-safe; keep one instance

try (Connection tx = dataSource.getConnection()) {
  tx.setAutoCommit(false);

  orders.markPaid(tx, orderId);
  outbox.write(tx, OutboxEvent.builder("orders", orderId, orderPaidJson)
      .header("type", "OrderPaid")
      .build());

  tx.commit(); // both rows, or neither
}
```

`OutboxWriter` never commits, rolls back or closes the connection; transaction control stays
with you. Other useful calls:

```java
outbox.writeAll(tx, events);                       // one JDBC batch
OutboxEvent.builder("reminders", userId, json)
    .deliverAfter(Instant.now().plus(Duration.ofHours(1)))   // delayed delivery
    .eventId(knownUuid)                                       // your own idempotency id
    .build();
```

#### Spring `@Transactional`

Use the connection bound to the current transaction:

```java
@Transactional
public void pay(String orderId) throws SQLException {
  orders.markPaid(orderId);
  Connection tx = DataSourceUtils.getConnection(dataSource);
  outbox.write(tx, OutboxEvent.builder("orders", orderId, json).build());
}
```

#### Any other language

The partition is computed by the database, so a plain `INSERT` is all you need:

```sql
INSERT INTO lego_outbox (destination, event_key, payload, headers, next_attempt_at)
VALUES ('orders', 'order-42', '{"status":"PAID"}', '{"type":"OrderPaid"}', now());
```

| Column | Required | Meaning |
|---|---|---|
| `destination` | yes | Name of a destination configured in the relay |
| `event_key` | yes | Ordering key, e.g. an aggregate id |
| `payload` | yes | Message body (text) |
| `headers` | no | JSON object of string headers, default `{}` |
| `next_attempt_at` | no | Earliest delivery time, default `now()` |
| `event_id` | no | UUID for de-duplication, generated if omitted |

### 3. Run the relay

```bash
docker build -t lego-relay .
docker run -p 8080:8080 -p 8081:8081 \
  -e LEGO_DB_URL=jdbc:postgresql://db:5432/app \
  -e LEGO_DB_USER=lego -e LEGO_DB_PASSWORD=secret \
  -e LEGO_KAFKA_BOOTSTRAP=kafka:9092 \
  -e LEGO_DESTINATIONS_ORDERS_TYPE=KAFKA \
  -e LEGO_DESTINATIONS_ORDERS_KAFKA_TOPIC=orders.v1 \
  lego-relay
```

Or run the jar directly: `java -jar lego-relay/target/lego-relay-*.jar`. Run as many instances
as you like against the same database; they split the partitions between themselves.

### 4. Consume safely

Delivery is *at least once*, so consumers should de-duplicate on the event id:

| Destination | Event id | Key |
|---|---|---|
| Kafka | `lego-event-id` record header | record key = `event_key` |
| HTTP | `Lego-Event-Id` request header | `Lego-Event-Key` request header |

Your own event headers are forwarded as Kafka headers or HTTP headers.

## Configuration

The relay is a Spring Boot application, so every property can be set in `application.yml` or as
an environment variable (`lego.retry.max-attempts` → `LEGO_RETRY_MAX_ATTEMPTS`).

### Destinations

Destinations live in configuration, so they are version-controlled and validated at startup. An
event whose `destination` isn't configured is dead-lettered with a clear error.

```yaml
lego:
  destinations:
    orders:                      # matches lego_outbox.destination
      type: KAFKA
      kafka:
        topic: orders.v1
        send-timeout: 10s        # wait for the broker ack
    billing-webhook:
      type: HTTP
      http:
        url: https://billing.example.com/hooks/orders
        method: POST
        timeout: 5s
        connect-timeout: 5s
        headers:
          Authorization: Bearer ${BILLING_TOKEN}
    audit:
      type: LOG                  # just logs the event; handy locally
```

How HTTP responses are classified:

| Outcome | Result |
|---|---|
| `2xx` | Delivered and deleted |
| `408`, `429`, `5xx`, timeout, connection error | Retried with backoff |
| Any other status (`400`, `401`, `404`, `422`, …) | Dead-lettered immediately |
| Invalid or restricted header name (e.g. `Host`) | Dead-lettered immediately |

### All properties

| Property | Default | Description |
|---|---|---|
| `lego.retry.max-attempts` | `10` | Attempts before an event is dead-lettered |
| `lego.retry.initial-backoff` | `1s` | Delay after the first failure |
| `lego.retry.multiplier` | `2.0` | Backoff growth factor (1s, 2s, 4s, …) |
| `lego.retry.max-backoff` | `10m` | Upper bound on the delay |
| `lego.polling.interval` | `200ms` | How often each instance looks for work |
| `lego.polling.batch-size` | `100` | Events fetched per partition per batch |
| `lego.polling.worker-threads` | `8` | Partitions processed in parallel per instance |
| `lego.polling.max-consecutive-batches` | `10` | Batches per partition before yielding to others |
| `lego.leasing.lease-duration` | `30s` | How long a crashed instance's partitions stay blocked |
| `lego.leasing.renew-interval` | `5s` | Lease renewal and rebalance period (≤ half the lease) |
| `lego.instance-id` | hostname + random suffix | Unique name of this instance |
| `lego.shutdown-timeout` | `30s` | Grace period for in-flight batches on shutdown |
| `lego.admin.write-enabled` | `false` | Enable the replay and discard endpoints |

Infrastructure is configured through standard Spring properties, with these environment
shortcuts:

| Variable | Default |
|---|---|
| `LEGO_DB_URL` | `jdbc:postgresql://localhost:5432/lego` |
| `LEGO_DB_USER` / `LEGO_DB_PASSWORD` | `lego` / `lego` |
| `LEGO_KAFKA_BOOTSTRAP` | `localhost:9092` |
| `PORT` | `8080` (app and admin API) |
| `MANAGEMENT_PORT` | `8081` (actuator) |

> **Kafka timeouts:** the producer's own blocking time (`max.block.ms` + `delivery.timeout.ms`,
> 3s + 7s by default) must stay at or below each destination's `send-timeout`. If you raise one,
> raise the other.

## Guarantees

| Property | Guarantee |
|---|---|
| **Atomicity** | An event exists if and only if its transaction committed. |
| **Delivery** | At least once. A crash between the destination's ack and the delete causes a redelivery. |
| **Ordering** | Strict per `(destination, event_key)`, including across failures and instances. None across keys. |
| **Failure isolation** | A failing event blocks only later events of the same key on the same destination. |
| **Poison messages** | Dead-lettered after `max-attempts` (or at once, if permanent), which unblocks the key. |
| **Ownership** | One instance processes a partition at a time; stale instances are fenced. |
| **Latency** | About `polling.interval` when healthy. After a crash, that instance's partitions resume within `lease-duration`. |

Lego **does not** give you exactly-once delivery, which no relay can promise against an external
system. Make consumers idempotent using the event id.

## Operations

### Admin API

Served on the application port (`8080` by default).

| Endpoint | Description |
|---|---|
| `GET /admin/stats` | Pending and dead counts and the oldest pending event, per destination |
| `GET /admin/dead-events?destination=&after=&limit=` | Dead letters with keyset paging (pass the last `id` as `after`; `limit` 1–500, default 50) |
| `POST /admin/dead-events/replay` | Re-queue dead events with a fresh retry budget † |
| `POST /admin/dead-events/discard` | Permanently delete dead events † |

† Off by default; enable with `lego.admin.write-enabled=true`. Both take
`{"eventIds": ["<uuid>", ...]}` (up to 1,000) and return `{"affected": n}`.

```bash
curl -s 'localhost:8080/admin/dead-events?destination=billing-webhook&limit=10'
curl -s -X POST localhost:8080/admin/dead-events/replay \
  -H 'Content-Type: application/json' \
  -d '{"eventIds":["3f2b8c4e-0a8e-4b8e-9d65-0c1d3e2f4a5b"]}'
```

> **Security:** the admin API has no built-in authentication. Keep it on a private network or
> behind an authenticating gateway. See [SECURITY.md](SECURITY.md).

### Metrics

Exposed at `GET :8081/actuator/prometheus`, and tagged with `destination`:

| Metric | Type | Meaning |
|---|---|---|
| `lego_events_delivered_total` | counter | Events delivered |
| `lego_events_retried_total` | counter | Failed attempts that were rescheduled |
| `lego_events_dead_total` | counter | Events moved to `DEAD` |
| `lego_delivery_send_seconds` | timer | Time spent in the sink |
| `lego_delivery_latency_seconds` | timer | Time from insert to successful delivery |

Suggested alerts: `increase(lego_events_dead_total[5m]) > 0`, and a growing oldest-pending age from
`/admin/stats`.

### Health

`/actuator/health/liveness` and `/actuator/health/readiness` on the management port, ready for
Kubernetes probes.

## How it works

1. **Partitioning.** `partition_no` is a generated column: `lego_partition_of(event_key)` maps
   each key to one of 64 partitions, so all events of a key share a partition.
2. **Leasing.** Every `renew-interval`, each instance heartbeats, renews its leases, and either
   releases partitions beyond its fair share or claims free and expired ones
   (`FOR UPDATE SKIP LOCKED`). There's no leader and no rebalance protocol.
3. **Delivery.** One worker thread at a time processes a partition, in `id` order. The poll
   query skips events that have an earlier pending event of the same destination and key still
   in backoff, which is what keeps per-key order across retries.
4. **Fencing.** Deletes and updates only apply while `owner = me AND generation = g`, so a stale
   instance's writes become no-ops and the new owner simply redelivers.

The full write-up, including trade-offs, is in [docs/DESIGN.md](docs/DESIGN.md).

## When to use Lego (and when not)

**Lego is a good fit if:**

- you're on PostgreSQL and want reliable event publishing without adding new infrastructure;
- you need per-entity ordering (per order, per customer…) and clear failure handling;
- you want something small enough to read and reason about end to end.

**Look elsewhere if:**

- you need very high throughput (hundreds of thousands of events per second). Log-based change
  data capture such as [Debezium](https://debezium.io/) reads the WAL instead of polling a table;
- you're not on PostgreSQL. Lego relies on Postgres-specific features;
- you want to publish *every* row change automatically rather than explicit events. That is CDC,
  not an outbox.

| | Lego | Log-based CDC (e.g. Debezium) | "Save, then publish" |
|---|---|---|---|
| Atomic with your data | Yes | Yes | **No** |
| Extra infrastructure | None (relay only) | Kafka Connect + connector | None |
| Event shape | Explicit, designed by you | Row changes | Explicit |
| Per-key ordering under failure | Yes | Depends on setup | No |
| Dead-letter + replay API | Built in | Via Kafka tooling | No |
| Throughput ceiling | One database | Very high | Broker limit |

## FAQ

<details>
<summary><b>Why poll instead of using LISTEN/NOTIFY or logical replication?</b></summary>

Polling a well-indexed partial index is simple, survives restarts without bookkeeping, and gives
predictable load. The default 200 ms interval keeps latency low. NOTIFY could be added later as a
wake-up hint, but correctness would still come from polling.
</details>

<details>
<summary><b>Will the outbox table grow forever?</b></summary>

No. Delivered events are deleted straight away, so the table only holds pending and dead events.
Dead events stay until you replay or discard them. Keep an eye on the `dead` count in
`/admin/stats`.
</details>

<details>
<summary><b>What happens if a relay instance crashes?</b></summary>

Its partitions stop being renewed. After `lease-duration` (30s by default) other instances claim
them and carry on. Events that were sent but not yet deleted are delivered again, which is why
consumers de-duplicate on the event id. On a *clean* shutdown, partitions are released
immediately.
</details>

<details>
<summary><b>Can I have more than 64 partitions?</b></summary>

64 is fixed in the initial migration, which keeps leasing trivial and is plenty for most
workloads. Since parallelism comes from partitions, at most 64 partitions are worked on at once
across all instances. Changing it means a new migration that updates both the partition function
and the seed rows.
</details>

<details>
<summary><b>How big can a payload be?</b></summary>

Payloads are stored as `text`, and Kafka's default record limit is about 1 MB. For large data,
store the blob elsewhere (object storage, a table) and put a reference in the event.
</details>

<details>
<summary><b>Does a delayed event (<code>deliverAfter</code>) block its key?</b></summary>

Yes. Later events of the same destination and key wait for it, which preserves insertion order.
Use a different key if the events are independent.
</details>

## Roadmap

Ideas, not promises. Discussion is welcome in the issues.

- [ ] Publish `lego-client` to Maven Central
- [ ] Prebuilt container image on GitHub Container Registry
- [ ] Spring Boot starter for the client (`@Transactional`-aware writer)
- [ ] Gauges for pending events and oldest pending age
- [ ] `LISTEN/NOTIFY` wake-up to reduce idle polling
- [ ] Optional retention of delivered events for auditing

## Contributing

Contributions are very welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) first. Building
needs only JDK 21 and Maven, because tests use an embedded PostgreSQL:

```bash
mvn verify
```

Please follow the [Code of Conduct](CODE_OF_CONDUCT.md), and report vulnerabilities privately as
described in [SECURITY.md](SECURITY.md). Changes are recorded in the [CHANGELOG](CHANGELOG.md).

## License

Lego is licensed under the [Apache License 2.0](LICENSE).
