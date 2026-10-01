# Contributing to Lego

Thanks for taking the time to contribute! Bug reports, docs fixes and code are all welcome.

## Ground rules

- **Open an issue first** for anything bigger than a small fix, so we can agree on the approach
  before you spend time on it.
- **Keep it simple.** Lego deliberately does one thing. Features that add a new runtime
  dependency, a new database, or a new coordination mechanism need a strong case.
- **Correctness beats speed.** Changes to delivery, ordering or leasing must come with tests that
  would fail without the change.

## Development setup

Requirements: **JDK 21** and **Maven 3.9+**. That's it: tests use an embedded PostgreSQL binary,
so Docker is not needed to build or test.

```bash
git clone https://github.com/yashrenhiet/Lego.git
cd Lego
mvn verify
```

To try the whole stack (PostgreSQL, Kafka, two relays) you need Docker:

```bash
./scripts/smoke-test.sh    # starts compose, sends an event, checks it lands on Kafka, tears down
docker compose up --build  # or keep it running and play
```

## Project layout

| Path | What lives there |
|---|---|
| `lego-client/` | Zero-dependency writer + the Flyway schema (`src/main/resources/db/lego`) |
| `lego-relay/.../lease` | Partition leasing and fencing |
| `lego-relay/.../outbox` | All SQL against `lego_outbox` |
| `lego-relay/.../delivery` | Engine, per-partition processor, retry policy, metrics |
| `lego-relay/.../sink` | Kafka, HTTP and log sinks |
| `lego-relay/.../admin` | Stats and dead-letter API |
| `docs/DESIGN.md` | How it works and why |

## Making a change

1. Fork and create a branch from `main`.
2. Write the test first where practical. Database behaviour is tested against real PostgreSQL
   (see `RelayFixture`); please don't mock SQL.
3. Run `mvn verify` and make sure it is green.
4. Update `README.md` / `docs/DESIGN.md` if behaviour or configuration changed.
5. Add a line under **Unreleased** in `CHANGELOG.md`.
6. Open a pull request describing **what** changed and **why**.

### Schema changes

Never edit an existing migration under `db/lego`. Add a new `V<n>__description.sql`; existing
installations have already applied the old ones.

### Code style

- Follow the existing style (Google-ish Java, 2-space indent, 100-column lines).
- Constructor injection, immutable records for data, no static mutable state.
- Log with placeholders (`log.info("x={}", x)`), never string concatenation.
- Catch specific exceptions. A broad `catch (RuntimeException e)` needs a comment saying why.

### Commit messages

Use the imperative mood and explain the *why* in the body:

```
Scope ordering to (destination, key)

A failing webhook must not stall Kafka events that share its key.
```

## Reporting bugs

Please include the Lego version, PostgreSQL version, relevant configuration (redact secrets),
logs, and the smallest set of steps that reproduces the problem.

For security issues, **do not open a public issue**; see [SECURITY.md](SECURITY.md).

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating you agree
to uphold it.
