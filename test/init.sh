#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUST_DIR="$(dirname "$SCRIPT_DIR")"
PID_FILE="/tmp/resonate-rust-test.pid"
# The gateway's default. Overridable, because this publishes a host port.
PORT="${PORT:-8001}"

if [ -f "$PID_FILE" ]; then
  echo "Server already running (PID $(cat $PID_FILE)). Run stop.sh first."
  exit 1
fi

# Start postgres with host port mapping (the docker-compose doesn't publish 5432)
echo "Starting PostgreSQL..."
docker run -d --name resonate-test-postgres \
  -e POSTGRES_USER=resonate \
  -e POSTGRES_PASSWORD=resonate \
  -e POSTGRES_DB=resonate \
  -p 5432:5432 \
  postgres:16-alpine

echo "Waiting for PostgreSQL to be ready..."
until docker exec resonate-test-postgres pg_isready -U resonate -q 2>/dev/null; do
  sleep 1
done

# Build
echo "Building resonate server..."
cd "$RUST_DIR"
cargo build --release

# Start server in background
#
# Migrations run here rather than at every restart: a fresh container has an
# empty schema, and this is the one place that knows it is fresh.
echo "Starting resonate server on :$PORT..."
RESONATE_SERVERS__ACTIVE=server_postgres \
RESONATE_SERVERS__SERVER_POSTGRES__URL="postgres://resonate:resonate@localhost:5432/resonate" \
RESONATE_SERVERS__SERVER_POSTGRES__POOL_SIZE=20 \
RESONATE_SERVERS__SERVER_POSTGRES__MIGRATE=true \
RESONATE_GATEWAYS__GATEWAY_HTTP__BIND="0.0.0.0:$PORT" \
RESONATE_DEBUG=true \
RESONATE_LEVEL=info \
./target/release/resonate serve &

echo $! > "$PID_FILE"

# Wait for the gateway rather than guessing at a sleep: a cold build plus a
# migration is not two seconds.
for _ in $(seq 1 60); do
  if curl -sf -o /dev/null "http://localhost:$PORT/ready"; then
    echo "Server is up at http://localhost:$PORT"
    exit 0
  fi
  sleep 1
done

echo "Server did not become ready at http://localhost:$PORT" >&2
exit 1
