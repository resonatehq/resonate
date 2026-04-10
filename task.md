# Task: Port Linearizability & Version Fixes from resonate-server-rs to resonate

## Goal

Transfer all database interaction fixes (linearizability + version-only-on-claim logic) from
`resonate-server-rs` (branch `version`) to `resonate` (branch `fix`).

After porting, the resonate server must pass the linearizability checker at the same rate as
the resonate-server-rs version branch (~999/1000 on PostgreSQL, ~200/200 on SQLite).

## Context

The fixes were developed and tested in:
```
/Users/dominiktornow/Developer/Vibes/resonate-agents-server/resonate-server-implementation/resonate-server-rs/.worktrees/version/
```

The target is:
```
/Users/dominiktornow/Developer/Vibes/resonate-agents-server/resonate-server-implementation/resonate/.worktrees/fix/
```

The linearizability checker binary is at:
```
/Users/dominiktornow/Developer/Vibes/resonate-agents-server/resonate-server-implementation/resonate-server-rs/.worktrees/version/test/resonate-test/linearizability-checker/linearizability-checker
```

### What changed in resonate-server-rs (version branch)

The `resonate/src/persistence/` files are identical to `resonate-server-rs/.worktrees/dst/src/persistence/`.
The diff `dst → version` captures exactly our changes. This means persistence patches can be
applied directly to resonate since they share the same base.

**Persistence changes (can be patched directly):**
- `persistence/mod.rs` — added `lock_for_update` and `process_callbacks` trait methods
- `persistence/persistence_postgres.rs` — linearizability fixes:
  - Added `lock_for_update` (FOR UPDATE locking on promise + task rows)
  - Added `process_callbacks` (fires callbacks as separate statement for fresh snapshot)
  - All `resumed_tasks` CTEs: removed `version = version + 1`
  - `task_acquire` CTE: added `version = version + 1`
  - `task_create` CTE: version = 1 for acquired, 0 for fulfilled
  - All operations: lock preamble before CTE for READ COMMITTED correctness
- `persistence/persistence_sqlite.rs` — version logic + fixes:
  - All resume/release/continue paths: removed `version = version + 1`
  - Both acquire paths: added `version = version + 1`
  - `task_create` INSERT: version = 1 for acquired, 0 for fulfilled
  - Fixed `lock_for_update` to check actual existence (was stub returning true)
  - Removed duplicate acquire from `task_create` (server handler does it)

**Server changes (need manual porting — resonate server.rs has diverged):**
- Lock preambles before try_timeout in: task.acquire, task.release, task.suspend, task.fulfill, task.fence
- `task.acquire` response: return `version: r.version + 1` (not `r.version`)
- `task.create` response: return `version: 1` for acquired, `version: t.version + 1` for retry acquire
- `task.create` handler: `process_callbacks` call when promise is settled
- `task.suspend` handler: use `lock_for_update` result for 404 vs 409

**Types changes:**
- Add `"2026-04-01"` to `SUPPORTED_VERSIONS`

## Steps

### Step 1: Generate and apply persistence patch
Generate diff `dst → version` for `src/persistence/` and apply to resonate fix worktree.
Verify the patch applies cleanly.

### Step 2: Port server.rs changes
Diff `dst → version` for `src/server.rs` to identify all DB-interaction changes.
Manually apply each change to resonate's `server.rs`, adapting as needed for any
divergence in the resonate codebase. Key changes to port:
1. Lock preambles (lock_for_update + try_timeout ordering) in each task handler
2. Version response fixes in task.acquire and task.create
3. process_callbacks call in task.create
4. 404 vs 409 logic using lock_for_update in task.suspend

### Step 3: Port types.rs change
Add `"2026-04-01"` to SUPPORTED_VERSIONS.

### Step 4: Build and verify
Build the resonate server. Fix any compilation errors.

### Step 5: Run linearizability tests (PostgreSQL)
Run `task-test.sh postgres`. Target: 50/50 then ~999/1000.
**Port PostgreSQL first and verify it passes before touching SQLite.**

### Step 6: Port SQLite persistence
Apply the SQLite-specific changes from the persistence patch (step 1 already applied it,
but verify). Key SQLite changes:
- All resume/release/continue paths: removed `version = version + 1`
- Both acquire paths: added `version = version + 1`
- `task_create` INSERT: version = 1 for acquired, 0 for fulfilled
- Fixed `lock_for_update` to check actual existence (was stub returning true)
- Removed duplicate acquire from `task_create` (server handler does it)

### Step 7: Run linearizability tests (SQLite)
Run `task-test.sh` (defaults to SQLite). Target: 50/50 then 1000/1000 (or close).

## Instructions for the agent

**IMPORTANT: The resonate-server-rs version branch is the source of truth. It has been
extensively tested and verified (999/1000 postgres, 200/200 sqlite). If you encounter
linearizability failures after porting, the problem is almost certainly an incorrect port —
NOT a bug in the source logic. Do not try to "fix" the logic. Instead, go back and compare
your port against the resonate-server-rs version branch line by line. We spent a very large
amount of time getting resonate-server-rs right. Trust it.**

- **Port order: PostgreSQL first, then SQLite.** Get postgres passing before touching sqlite.
- Keep a log of progress in `task.jsonl`. Each entry is one line of JSON:
  `{"step": N, "action": "description", "status": "done|in_progress|blocked", "description": "details"}`
- Commit after each step completes.
- When applying patches, verify the diff makes sense before committing.
- When porting server.rs, work handler-by-handler. Don't try to apply a monolithic patch.
- Use `task-test.sh` for all testing. If 50 iterations fail, fix before running 1000.
- If a step is blocked, document why in task.jsonl and move on if possible.
- When debugging failures, always diff against resonate-server-rs version branch first.
