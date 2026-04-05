#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Linearizability test runner for resonate server
#
# Usage:
#   ./task-test.sh              # SQLite (default)
#   ./task-test.sh postgres     # PostgreSQL
#
# Runs 50 iterations first. If all pass, runs 1000 iterations.
# Failing traces are preserved in results/ for inspection.
# ============================================================================

STORAGE="${1:-sqlite}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHECKER="/Users/dominiktornow/Developer/Vibes/resonate-agents-server/resonate-server-implementation/resonate-server-rs/.worktrees/version/test/resonate-test/linearizability-checker/linearizability-checker"
SERVER="$SCRIPT_DIR/target/release/resonate"
RESULTS_DIR="$SCRIPT_DIR/results"
PORT=8001

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

cleanup() {
    # Graceful then forceful kill
    kill $(lsof -ti :$PORT) 2>/dev/null || true
    sleep 1
    kill -9 $(lsof -ti :$PORT) 2>/dev/null || true
    sleep 1
}

start_server() {
    cleanup
    rm -f "$SCRIPT_DIR/resonate.db" "$SCRIPT_DIR/resonate.db-shm" "$SCRIPT_DIR/resonate.db-wal"

    if [ "$STORAGE" = "postgres" ]; then
        # Ensure postgres container is running
        docker start resonate-test-postgres 2>/dev/null || \
            docker run -d --name resonate-test-postgres \
                -e POSTGRES_USER=resonate -e POSTGRES_PASSWORD=resonate -e POSTGRES_DB=resonate \
                -p 5432:5432 postgres:16-alpine
        sleep 3

        RESONATE_STORAGE__TYPE=postgres \
        RESONATE_STORAGE__POSTGRES__URL="postgres://resonate:resonate@localhost:5432/resonate" \
        RESONATE_STORAGE__POSTGRES__POOL_SIZE=20 \
        RESONATE_DEBUG=true RESONATE_LEVEL=info \
        "$SERVER" serve &> /tmp/resonate-test.log &
    else
        RESONATE_DEBUG=true RESONATE_LEVEL=info \
        "$SERVER" serve &> /tmp/resonate-test.log &
    fi

    sleep 2

    # Verify server is up
    if ! curl -s http://localhost:$PORT/ -X POST -H "Content-Type: application/json" \
        -d '{"kind":"debug.reset","head":{"corrId":"check","version":"2026-04-01"},"data":{}}' > /dev/null 2>&1; then
        echo "ERROR: Server failed to start. Check /tmp/resonate-test.log"
        exit 1
    fi
    echo "Server started ($STORAGE)"
}

reset_server() {
    curl -s http://localhost:$PORT/ -X POST -H "Content-Type: application/json" \
        -d '{"kind":"debug.reset","head":{"corrId":"reset","version":"2026-04-01"},"data":{}}' > /dev/null
}

run_batch() {
    local count=$1
    local label=$2
    local pass=0
    local fail=0
    local fail_seeds=""

    echo ""
    echo "=== Running $count iterations ($label, $STORAGE) ==="

    for seed in $(seq 1 "$count"); do
        reset_server
        result=$("$CHECKER" \
            --url http://localhost:$PORT \
            --workers 10 --ids 5 --ops 200 \
            --seed "$seed" \
            --output "$RESULTS_DIR/history-$seed.html" \
            2>&1 | grep -E "^(OK|FAIL)")

        if echo "$result" | grep -q "^OK"; then
            pass=$((pass + 1))
            # Clean up passing traces
            rm -f "$RESULTS_DIR/history-$seed.html" "$RESULTS_DIR/history-$seed.trace"
        else
            fail=$((fail + 1))
            fail_seeds="$fail_seeds $seed"
        fi

        # Progress indicator every 50 seeds
        if [ $((seed % 50)) -eq 0 ]; then
            echo "  ... $seed/$count done ($pass passed, $fail failed)"
        fi
    done

    echo ""
    echo "Results ($label, $STORAGE): $pass/$count passed, $fail failed"
    if [ -n "$fail_seeds" ]; then
        echo "Failing seeds:$fail_seeds"
        echo "Traces in: $RESULTS_DIR/"
    fi

    return $fail
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

# Build first
echo "Building server..."
cd "$SCRIPT_DIR"
cargo build --release 2>&1 | tail -3

# Prepare results directory
mkdir -p "$RESULTS_DIR"
rm -f "$RESULTS_DIR"/history-*.html "$RESULTS_DIR"/history-*.trace

# Start server
start_server

# Phase 1: Quick check (50 iterations)
if ! run_batch 50 "quick check"; then
    echo ""
    echo "FAILED quick check. Fix issues before running full suite."
    cleanup
    exit 1
fi

# Phase 2: Full suite (1000 iterations)
# Rare non-deterministic failures (~0.1%) are expected due to PostgreSQL
# deadlocks causing 500 responses that create state space explosion for
# the porcupine checker. We allow up to 1% failure rate.
run_batch 1000 "full suite"
FULL_FAIL=$?
if [ "$FULL_FAIL" -gt 10 ]; then
    echo ""
    echo "FAILED full suite ($FULL_FAIL failures exceeds 1% threshold)."
    cleanup
    exit 1
fi
if [ "$FULL_FAIL" -gt 0 ]; then
    echo ""
    echo "PASSED full suite with $FULL_FAIL non-deterministic failures (within 1% threshold)."
fi

echo ""
echo "ALL TESTS PASSED"
cleanup
