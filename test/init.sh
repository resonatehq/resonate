#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUST_DIR="$(dirname "$SCRIPT_DIR")"
PID_FILE="/tmp/resonate-rust-test.pid"

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
echo "Starting resonate server on :3000..."
RESONATE_STORAGE__TYPE=postgres \
RESONATE_STORAGE__POSTGRES__URL="postgres://resonate:resonate@localhost:5432/resonate" \
RESONATE_STORAGE__POSTGRES__POOL_SIZE=20 \
RESONATE_DEBUG=true \
RESONATE_LEVEL=info \
./target/release/resonate serve &

echo $! > "$PID_FILE"
sleep 2

# Health check
if curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/health | grep -q "200"; then
  echo "Server is up. Run test.sh http://localhost:3000"
else
  echo "Warning: server may not be ready yet"
fi
