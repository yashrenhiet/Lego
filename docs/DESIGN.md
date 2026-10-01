# Design

## Data model

`lego_outbox` holds one row per undelivered event. `partition_no` is a generated column,
`lego_partition_of(event_key)` (first 28 bits of md5, mod 64), so producers never compute it and
any language can insert with plain SQL.

| status | meaning |
|---|---|
| `PENDING` | waiting for delivery (possibly in backoff: `next_attempt_at > now()`) |
| `DEAD` | permanent failure or retries exhausted; kept until replayed or discarded |

Delivered events are **deleted**, so the table stays small and the hot index stays in memory.

## Partition leasing (no leader)

`lego_partition` has 64 rows: `owner`, `lease_until`, `generation`. Every `renew-interval`, each
instance:

1. heartbeats into `lego_instance` and deletes instances whose heartbeat is older than the lease;
2. computes `fairShare = ceil(64 / liveInstances)`;
3. renews leases it still holds (expired ones are *not* revived);
4. if it holds more than its share, releases the extras; if fewer, claims unowned or expired
   partitions with `FOR UPDATE SKIP LOCKED`, incrementing `generation`.

Instances converge to an even split within a couple of ticks. On clean shutdown an instance
releases everything, so others pick its partitions up on their next tick.

Each instance treats a lease as locally valid only until `lease_until - renew-interval`, a
safety margin against clock drift and slow renewals.

## Fencing

Losing a lease must not let an instance keep writing. Every delete or update the relay makes is
prefixed with:

```sql
WITH fence AS (SELECT 1 FROM lego_partition
                WHERE partition_no = :p AND owner = :me AND generation = :g AND lease_until > now()
                  FOR SHARE)
```

and only applies `WHERE ... AND EXISTS (SELECT 1 FROM fence)`. The `FOR SHARE` lock makes a
concurrent claim (which needs `FOR UPDATE`) wait until the write commits. A stale instance's
writes therefore become no-ops and the event is simply redelivered by the new owner, which is
fine under at-least-once.

## Ordering

Within a partition, a single worker thread processes events in `id` order (the engine never runs
the same partition twice concurrently). The poll query skips any event that has an **earlier
pending event with the same key still in backoff**:

```sql
AND NOT EXISTS (SELECT 1 FROM lego_outbox earlier
                 WHERE earlier.event_key = o.event_key AND earlier.status = 'PENDING'
                   AND earlier.id < o.id AND earlier.next_attempt_at > now())
```

Inside a batch, once an event is rescheduled its key is added to a `blockedKeys` set. So a failing
event holds back only its own key, across batches and across instances, until it succeeds or
goes `DEAD`. Dead-lettering unblocks the key: a poison message doesn't stall a key forever.

Note this also means an event scheduled with `deliverAfter` in the future holds back later
events of the same key, which keeps the "insertion order" promise.

## Delivery loop

```
every poll interval:
  for each leased partition not already in flight:
    worker: up to 10 batches while batches are full and the lease holds
      batch = findDeliverable(partition, batchSize)
      for event in batch:
        if key blocked: skip
        send -> ok:   collect id
             -> fail: RetryPolicy -> scheduleRetry (block key) | markDead
      deleteDelivered(collected ids)        -- one statement per batch
```

Sinks block until the downstream ack, with a bounded timeout. Kafka uses `acks=all` plus the
idempotent producer, and the record key equals the event key, so Kafka preserves per-key order too.

## Trade-offs and non-goals

- **PostgreSQL only.** The partition function, `SKIP LOCKED` and generated columns are
  Postgres-specific. A MySQL port is possible but not a goal.
- **Fixed 64 partitions.** That's plenty for most workloads and the reason there is no rebalancing
  protocol to get wrong. To change it, edit the migration: the function modulus and the seed row count.
- **Throughput** is bounded by a single database. Expect thousands of events per second, not
  millions. That is the right trade for "never lose an event".
- **Payload size**: payloads are `text` in the row. Store large blobs elsewhere and put a
  reference in the event.
- **No built-in auth** on the admin API.
