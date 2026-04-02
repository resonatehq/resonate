#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="/tmp/resonate-rust-test.pid"

if [ -f "$PID_FILE" ]; then
  PID=$(cat "$PID_FILE")
  echo "Stopping resonate server (PID $PID)..."
  kill "$PID" 2>/dev/null || true
  rm "$PID_FILE"
else
  echo "No PID file found, server may not be running"
fi

echo "Stopping PostgreSQL..."
docker rm -f resonate-test-postgres 2>/dev/null || true

echo "Done."
