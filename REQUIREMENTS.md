# Requirements: Embedded Web UI

## Goal

Serve a self-contained single-page application from the resonate binary with zero runtime file dependencies.

## Approach

Use `rust-embed` to bake the compiled SPA output into the binary at build time. Serve it via axum on `/web` and `/web/*path`.

## Product Requirements

### Views

**Default (Dashboard)**
- Top-level promises with a target, pending status, and whether actively worked on
- Entry point — clean signal, no noise

**All Promises**
- All promises regardless of state
- Filterable/sortable

**Failed Promises**
- Promises in failed state
- Actionable focus: retry or cancel inline

**Future: Repeated Task Drops**
- Track promises/tasks dropped repeatedly (not yet in system — needs server-side support)
- Placeholder view; implement once tracking data is available

### Actions

- From any promise view: inspect detail, retry, cancel
- Actions are contextual — only shown when applicable

### Visual Style

- Clean, minimal, no decoration for decoration's sake
- Only actionable information is shown
- No external UI libraries that add visual opinions (no Bootstrap, no MUI)

---

## Constraints

- No new runtime dependencies (no file system reads, no separate asset directory)
- No external CDN dependencies — fully self-contained in the binary
- Must not interfere with existing API routes (`/`, `/health`, `/ready`, `/poll/*`)
- Actions (retry, cancel) call existing API; no new server-side endpoints for phase 1

## Implementation Steps

### 1. Cargo.toml

Add:

```toml
rust-embed = { version = "8", features = ["mime-guess"] }
```

### 2. Embed assets

Create `web/dist/` (output of the SPA build). Add to `src/assets.rs`:

```rust
#[derive(rust_embed::Embed)]
#[folder = "web/dist/"]
struct Assets;
```

### 3. Axum handler

Add `GET /web` and `GET /web/*path` routes backed by a handler that:
1. Looks up the path in `Assets`
2. Returns 404 if not found
3. Falls back to `index.html` for unknown paths (client-side routing)
4. Sets `Content-Type` from the embedded mime guess
5. Sets `ETag` from the file hash for caching

### 4. Mount routes

In `main.rs`, mount the UI router alongside `api_routes()` and `poll_routes()`.

### 5. SPA

A minimal Vite + React app in `web/` that:
- Implements the views above against the existing JSON-RPC API at `/`
- Polls for live updates (no SSE required for phase 1)
- Has no build-time env vars — all API calls go to `/` (same origin)
- No external UI framework — plain CSS or CSS modules only

### 6. Build order

`npm run build` inside `web/` must run before `cargo build`. Document this in the repo README. CI must sequence accordingly.

## Acceptance Criteria

- `cargo build` produces a single binary
- `GET /web` returns the SPA shell (200, `text/html`)
- `GET /web/some/deep/route` returns `index.html` (client-side routing works)
- Static assets (`*.js`, `*.css`) return correct `Content-Type` and `ETag`
- Existing API routes are unaffected
- Binary size increase is under 10 MB
- Dashboard shows top-level pending promises and their active work status
- Failed promises view renders and retry/cancel actions call through to the API
