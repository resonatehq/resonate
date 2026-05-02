# Plan

## Build Command
```
docker compose -f .docker/docker-compose.yml --project-name experimental-web build
```

## Test Command
```
docker compose -f .docker/docker-compose.yml --project-name experimental-web up --build --abort-on-container-exit --exit-code-from test-runner
```

## Tasks

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1 | Set up isolated dev environment | done | `.docker/` with compose + Dockerfile + test-runner.sh |
| 2 | Scaffold Vite + React SPA in `web/` | done | Vite 5 + React 18 + React Router v6, builds to web/dist/ |
| 3 | Implement API client layer | done | `api.ts` with envelope unwrapping, corrected field names |
| 4 | Implement Dashboard view | done | Polls pending promises every 5s |
| 5 | Implement All Promises view | done | All states, filterable by state badge |
| 6 | Implement Failed Promises view | done | Rejected promises with retry/cancel |
| 7 | Implement Placeholder view (Repeated Task Drops) | done | Static placeholder |
| 8 | Wire up Promise Detail + retry/cancel actions | done | Contextual buttons via promise.settle |
| 9 | Add `rust-embed` to `Cargo.toml` | done | version = "8", features = ["mime-guess"] |
| 10 | Create `src/assets.rs` + axum handler | done | FNV-64 ETag, mime-guess Content-Type, SPA fallback |
| 11 | Mount `/web` and `/web/*path` routes in `main.rs` | done | web_routes() merged alongside api_routes() + poll_routes() |
| 12 | Final acceptance test pass | done | 6/6 smoke tests pass; release binary 23 MB (~175 KB from SPA) |

## Notes

- Worktree root: `.worktrees/experimental-web/`  
- Resonate API: single JSON-RPC endpoint at `POST /`  
- All promise queries use `promise.search`, all mutations use `promise.settle`  
- No new server-side endpoints in phase 1; retry/cancel call existing API ops  
- `npm run build` inside `web/` must precede `cargo build` — Dockerfile sequences this  
- `ETag` = SHA-256 hex of embedded file bytes (truncated to 32 chars)  
