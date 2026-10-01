-- Lego outbox schema (PostgreSQL 13+).
--
-- Events are spread over a FIXED number of partitions (64). All events with the same
-- event_key land in the same partition, and each partition is processed by exactly one
-- relay instance at a time, which gives strict per-key ordering.
--
-- The partition is computed by the database itself, so producers in any language can
-- write events with a plain INSERT.

CREATE FUNCTION lego_partition_of(event_key text) RETURNS integer
    LANGUAGE sql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
AS $$
    -- 28 bits keeps the value positive; md5 is stable across PostgreSQL versions.
    SELECT (('x' || substr(md5(event_key), 1, 7))::bit(28)::integer) % 64
$$;

CREATE TABLE lego_outbox
(
    id              bigserial PRIMARY KEY,
    event_id        uuid          NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    destination     varchar(200)  NOT NULL,
    event_key       varchar(500)  NOT NULL,
    partition_no    integer       NOT NULL GENERATED ALWAYS AS (lego_partition_of(event_key)) STORED,
    headers         jsonb         NOT NULL DEFAULT '{}'::jsonb,
    payload         text          NOT NULL,
    status          varchar(16)   NOT NULL DEFAULT 'PENDING'
        CONSTRAINT lego_outbox_status_chk CHECK (status IN ('PENDING', 'DEAD')),
    attempts        integer       NOT NULL DEFAULT 0,
    created_at      timestamptz   NOT NULL DEFAULT now(),
    next_attempt_at timestamptz   NOT NULL DEFAULT now(),
    last_error      varchar(2000)
);

-- Hot path: "next pending events of partition N, in insertion order".
CREATE INDEX lego_outbox_poll_idx ON lego_outbox (partition_no, id) WHERE status = 'PENDING';
-- Ordering guard: "is there an earlier pending event of this key still waiting to retry?".
CREATE INDEX lego_outbox_key_idx ON lego_outbox (event_key, id) WHERE status = 'PENDING';
-- Admin: browse dead letters per destination.
CREATE INDEX lego_outbox_dead_idx ON lego_outbox (destination, id) WHERE status = 'DEAD';

-- One row per partition. Relay instances lease partitions; `generation` is bumped on every
-- change of ownership and acts as a fencing token for writes.
CREATE TABLE lego_partition
(
    partition_no integer PRIMARY KEY,
    owner        varchar(200),
    lease_until  timestamptz,
    generation   bigint NOT NULL DEFAULT 0
);

INSERT INTO lego_partition (partition_no)
SELECT generate_series(0, 63);

-- Live relay instances, used to compute each instance's fair share of partitions.
CREATE TABLE lego_instance
(
    instance_id  varchar(200) PRIMARY KEY,
    heartbeat_at timestamptz NOT NULL
);
