#!/usr/bin/env bash
set -euo pipefail

BASE="${RESONATE_URL:-http://resonate:8001}"

passed=0
failed=0

check() {
  local label="$1"
  local url="$2"
  local expected_status="$3"
  local expected_ct="${4:-}"

  local response
  response=$(curl -si "${url}" 2>/dev/null)

  local status
  status=$(printf '%s' "$response" | head -1 | grep -oE '[0-9]{3}')

  if [ "$status" != "$expected_status" ]; then
    printf 'FAIL  %s — expected status %s, got %s\n' "$label" "$expected_status" "$status"
    failed=$((failed + 1))
    return
  fi

  if [ -n "$expected_ct" ]; then
    local ct
    ct=$(printf '%s' "$response" | grep -i '^content-type:' | head -1)
    if ! printf '%s' "$ct" | grep -qi "$expected_ct"; then
      printf 'FAIL  %s — expected content-type containing "%s", got: %s\n' "$label" "$expected_ct" "$ct"
      failed=$((failed + 1))
      return
    fi
  fi

  printf 'PASS  %s\n' "$label"
  passed=$((passed + 1))
}

check_post() {
  local label="$1"
  local url="$2"
  local body="$3"
  local expected_status="$4"

  local response
  response=$(curl -si -X POST -H 'Content-Type: application/json' -d "$body" "${url}" 2>/dev/null)

  local status
  status=$(printf '%s' "$response" | head -1 | grep -oE '[0-9]{3}')

  if [ "$status" != "$expected_status" ]; then
    printf 'FAIL  %s — expected status %s, got %s\n' "$label" "$expected_status" "$status"
    failed=$((failed + 1))
    return
  fi

  printf 'PASS  %s\n' "$label"
  passed=$((passed + 1))
}

# SPA routes — all should return 200 with text/html
check "GET /web"                "${BASE}/web"                200 "text/html"
check "GET /web/"               "${BASE}/web/"               200 "text/html"
check "GET /web/some/deep/route" "${BASE}/web/some/deep/route" 200 "text/html"

# Health / readiness
check "GET /health"             "${BASE}/health"             200
check "GET /ready"              "${BASE}/ready"              200

# API smoke
check_post "POST /" \
  "${BASE}/" \
  '{"kind":"promise.search","head":{"corrId":"smoke-1","version":"2026-04-01"},"data":{"limit":1}}' \
  200

printf '\n%d passed, %d failed\n' "$passed" "$failed"

if [ "$failed" -gt 0 ]; then
  exit 1
fi
