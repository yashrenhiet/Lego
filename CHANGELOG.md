# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `lego-client`: zero-dependency `OutboxWriter` that writes events on the caller's JDBC
  connection, plus the PostgreSQL schema as a Flyway migration (`classpath:db/lego`).
- `lego-relay`: Spring Boot relay with leader-less partition leasing (64 partitions) and
  generation-fenced writes.
- Strict ordering per `(destination, key)`, including across retries and instances.
- Exponential backoff, dead-letter state, and permanent-failure classification for HTTP and Kafka.
- Kafka, HTTP and log sinks.
- Admin API: stats, keyset-paginated dead-letter listing, replay and discard (opt-in).
- Prometheus metrics for delivered, retried and dead events and end-to-end latency.
- Docker image, docker compose playground, and a CI smoke test of the quick start.

[Unreleased]: https://github.com/yashrenhiet/Lego/commits/main
