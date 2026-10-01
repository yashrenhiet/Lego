#!/usr/bin/env bash
# Runs the README "Quick start" end to end against docker compose:
# start the stack, insert an event with plain SQL, and assert it arrives on Kafka.
# CI runs this on every push, so the README's first command is known to work.
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE="docker compose"

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    echo "Smoke test failed; recent logs:" >&2
    $COMPOSE logs --tail=80 >&2 || true
  fi
  $COMPOSE down -v >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

wait_for() {
  local description=$1 timeout=$2
  shift 2
  echo "Waiting for $description..."
  for _ in $(seq 1 "$timeout"); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for $description" >&2
  return 1
}

$COMPOSE up -d --build

wait_for "relay-1 readiness" 180 curl -fs http://localhost:9081/actuator/health/readiness
wait_for "relay-2 readiness" 60 curl -fs http://localhost:9082/actuator/health/readiness

echo "Inserting an event with plain SQL..."
$COMPOSE exec -T postgres psql -v ON_ERROR_STOP=1 -U lego -d lego -c \
  "INSERT INTO lego_outbox (destination, event_key, payload) VALUES ('orders', 'order-42', '{\"status\":\"PAID\"}');"

echo "Consuming from orders.v1..."
message=$($COMPOSE exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic orders.v1 --from-beginning \
  --max-messages 1 --timeout-ms 60000 --property print.key=true 2>/dev/null)
echo "Received: $message"

case "$message" in
  *order-42*PAID*) ;;
  *) echo "Expected the order-42 event on orders.v1" >&2; exit 1 ;;
esac

wait_for "the outbox to drain" 30 sh -c \
  "curl -fs http://localhost:8081/admin/stats | grep -qx '\[\]'"

echo "Smoke test passed: SQL insert -> relay -> Kafka, outbox drained."
