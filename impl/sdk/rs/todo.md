# TODO — Rust SDK Parity with TS SDK

## 1. Improve OptionsBuilder to merge defaults with provided options

The TS `OptionsBuilder.build()` merges partial user options with defaults (`timeout=24h`, `target="default"`, `version=0`, etc.) and resolves the target through `network.match()`. The Rust version just returns `opts.unwrap_or_default()` with no merging or target resolution.

> **This is the highest priority item** — it unblocks #3, #8, and #9, which all depend on having a proper options merge with target resolution in a single place.

- [x] Change `OptionsBuilder::build()` to accept `Option<PartialOptions>` or individual optional fields instead of `Option<Options>`
- [x] Apply defaults for each field individually: if the user provides only `timeout`, the rest should still get proper defaults
- [x] Resolve `target` through `network.match()` inside `build()`, matching TS behavior (URL targets pass through, bare names go through `network.match`)
- [x] Set default target to `"default"` when none is provided (TS does this)
- [x] Remove ad-hoc target resolution from `Resonate::begin_rpc` — let `OptionsBuilder` handle it
- [x] Collapse the ad-hoc `is_url` / `network.match()` logic scattered in `begin_rpc` and the `match_fn` closure in `Resonate::new` into `OptionsBuilder::build()` as the single place for target resolution
- [x] Update all call sites (`Resonate::begin_run_by_name`, `Resonate::begin_rpc`, `Resonate::schedule`, `Context` methods) to use the improved builder
- [x] Add tests: partial options merge correctly (e.g. only `timeout` provided, rest are defaults), target resolution through `network.match()` works, URL targets pass through unchanged, default target is `"default"` when none specified

## 2. Change default timeout to 24 hours (match TS SDK)

The TS SDK defaults to `24 * HOUR` (86,400,000 ms). The Rust SDK defaults to 60 seconds.

> **Note**: `Duration::from_secs(86_400)` is 24 hours in *seconds*. The TS SDK uses milliseconds internally (`24 * HOUR` = 86,400,000 ms). Ensure the Rust code that converts `opts.timeout` to `timeout_at` uses `.as_millis()` consistently — it already does in `resonate.rs`, so this should be safe.

- [x] Update `Options::default()` in `resonate/src/options.rs` to use `Duration::from_secs(86_400)` instead of `Duration::from_secs(60)`
- [x] Update any tests that assert on the default timeout value (`options_returns_defaults_when_none` in `resonate.rs`)
- [x] Audit `Resonate::new` for any hardcoded timeout assumptions that conflict with the new default
- [x] Add tests: verify `Options::default().timeout` equals 24 hours, verify promises created without explicit timeout use the new default

## 3. Cap child promise timeouts to parent timeout

> **Depends on**: #1 (OptionsBuilder merge). If #1 is done first and an `Options` is threaded into Context methods, the timeout cap falls out naturally from the child's resolved options.

The TS SDK caps child timeouts to the parent's timeout: `Math.min(now + opts.timeout, parent.timeout)`. The Rust SDK just passes through the parent's `timeout_at` without considering the child's requested timeout.

- [x] Update `Context::local_create_req` to compute `timeout_at` as `min(now_ms() + child_requested_timeout, self.timeout_at)` instead of just `self.timeout_at`
- [x] Update `Context::remote_create_req` with the same capping logic
- [x] Accept a child timeout parameter (from `Options` or a duration) in `run`, `begin_run`, `rpc`, `begin_rpc` on `Context` and thread it through to the create request builders
- [x] For detached calls (if/when added), use `i64::MAX` as the cap instead of parent timeout (matching TS `maxTimeout` behavior)
- [x] Add tests: child timeout exceeding parent's is clamped to parent's `timeout_at`, child timeout smaller than parent's is respected as-is, both local (`run`/`begin_run`) and remote (`rpc`/`begin_rpc`) paths apply capping correctly, detached calls use `i64::MAX` cap

## 4. Start heartbeat when task is acquired in Core

The TS SDK calls `heartbeat.start()` in `Computation` before executing a function to keep task leases alive. The Rust SDK constructs `AsyncHeartbeat` but never starts it during execution. This should be done in `Core`, not in a separate computation layer.

> **Note**: The TS SDK starts the heartbeat *once* per computation, not per `execute_until_blocked` call. If a redirect causes re-execution (#5), the heartbeat should not be stopped and restarted on each iteration — start it before the loop, stop it after.

- [x] Pass the `Heartbeat` (or `Arc<dyn Heartbeat>`) into `Core::new`
- [x] Call `heartbeat.start()` once at the top of `execute_until_blocked`, before any potential redirect loop, after confirming the promise is pending
- [x] Call `heartbeat.stop()` in the cleanup path (both success and error) of `execute_until_blocked`, outside the redirect loop
- [x] Ensure `on_message` path (which calls `execute_until_blocked`) also benefits from heartbeat start/stop
- [x] Verify `NoopHeartbeat` (used in local mode) is a no-op for `start()`/`stop()` so local mode is unaffected
- [x] Add tests: heartbeat `start()` is called when a task begins execution, heartbeat `stop()` is called after task completes (both success and error paths), `NoopHeartbeat` does not interfere in local mode

## 5. Redirect should re-execute with preload, not re-acquire

The TS SDK's `suspendTask` returns `{ continue: true, preload }` on redirect, and the caller loops back into `executeUntilBlocked(task, rootPromise, preload)` — same already-acquired task, no re-acquire. The Rust SDK's `suspend_task` calls `self.on_message(task_id)` on redirect, which re-acquires the task from scratch. This causes an extra round-trip and a possible race condition if another worker acquires between release and re-acquire.

- [x] Change `suspend_task` to return the preloaded promises from the `Response::Redirect` variant instead of calling `self.on_message`
- [x] Update `execute_until_blocked_inner` (or `execute_until_blocked`) to loop on redirect: when `suspend_task` returns a redirect with preload, call `execute_until_blocked_inner` again with the same `task_id` and `root_promise` but the new preloaded promises
- [x] Remove the recursive `self.on_message(task_id)` call from `suspend_task`
- [x] Add tests: redirect re-executes without sending a second `TaskAcquire`, preloaded promises from redirect are passed to the next execution round, multiple consecutive redirects are handled correctly

## 6. Short-circuit on settled promise must still fulfill the task

The TS SDK's `processGenerator` returns a `Done` status with the settled value when the root promise is already settled, which flows back to `Core.executeUntilBlocked` → `fulfillTask(task, status)`. The Rust SDK's `execute_until_blocked_inner` returns `Ok(Status::Done)` without calling `fulfill_task` when short-circuiting on a settled promise. This leaves the task in "acquired" state forever (leaked task).

- [x] When short-circuiting on a settled root promise in `execute_until_blocked_inner`, call `fulfill_task(task_id, &root_promise.id, &result)` before returning `Ok(Status::Done)`
- [x] Extract the settled value from the root promise to pass to `fulfill_task` (resolved → `Ok(value)`, rejected → `Err(...)`)
- [x] Add tests: settled root promise still sends `TaskFulfill`, task does not remain in "acquired" state after short-circuit, both resolved and rejected settled promises trigger fulfillment

## 7. Store and cancel subscription refresh handle on `stop()`

The subscription refresh `JoinHandle` is immediately dropped after spawning (`drop(refresh_handle)` in `Resonate::new`), so `stop()` cannot cancel the background refresh task. The task runs forever even after `stop()` is called.

- [x] Store the `refresh_handle` in `self.subscription_refresh_handle` instead of dropping it
- [x] Verify `stop()` aborts the refresh handle (it already has the logic, but the handle is always `None`)
- [x] Add tests: after `stop()`, the subscription refresh task is no longer running, `stop()` is idempotent (calling twice doesn't panic)

## 8. Remove custom ID option from Context calls

The TS SDK allows custom IDs on context calls (`opts.id`), which introduces `breaksLineage` logic to reset `resonate:origin` when the user overrides the ID. The Rust SDK's `local_create_req` and `remote_create_req` always set `origin = self.origin_id` — lineage never breaks regardless. Instead of replicating the complex lineage-breaking logic, remove custom IDs from Context-level calls entirely. Context calls (`run`, `begin_run`, `rpc`, `begin_rpc`) should always use deterministic sequential IDs (`parent.N`). Custom IDs remain available only at the top-level `Resonate` API (`Resonate::run`, `Resonate::begin_run`, `Resonate::rpc`, etc.).

- [x] Ensure `Context::run`, `Context::begin_run`, `Context::rpc`, `Context::begin_rpc` do not accept an ID parameter — IDs are always generated via `self.next_id()`
- [x] Verify that `local_create_req` and `remote_create_req` always use `self.origin_id` for `resonate:origin` (no lineage-breaking path needed)
- [x] Confirm the top-level `Resonate` API still requires the user to provide an explicit ID for `run`, `begin_run`, `rpc`, `begin_rpc`
- [x] Document this as an intentional divergence from TS: Context calls are always deterministic, custom IDs are a top-level concern only
- [x] Add tests: sequential Context calls produce deterministic IDs (`root.0`, `root.1`, ...), `resonate:origin` always matches the root ID for all nested calls, top-level `Resonate` calls still use user-provided IDs

## 9. `begin_run_by_name` should use resolved target, not `network.anycast()`

> **Depends on**: #1 (OptionsBuilder merge). After #1 is done, both `begin_run_by_name` and `begin_rpc` should get target resolution from `OptionsBuilder::build()`.

The TS SDK uses `opts.target` (resolved through `network.match()`) for the `resonate:target` tag in `beginRun`. The Rust SDK's `begin_run_by_name` hardcodes `self.network.anycast()` as the target, ignoring any user-provided target option.

- [x] Replace `self.network.anycast().to_string()` in `begin_run_by_name` with the resolved target from opts (URL targets pass through, bare names go through `network.match()`)
- [x] Ensure consistency with `begin_rpc` which already resolves the target via `is_url` / `network.match()`
- [x] After TODO #1 (OptionsBuilder merge) is done, both `begin_run_by_name` and `begin_rpc` should get target resolution from `OptionsBuilder::build()`
- [x] Add tests: `begin_run_by_name` with custom target option uses that target, default target uses `network.match("default")`, URL target passes through unchanged

## 10. Context `rpc`/`begin_rpc` should use resolved target option

> **Depends on**: #1 (OptionsBuilder merge). Once Context methods accept options, target resolution comes from the builder.

The TS SDK's context-level `rpc` and `rfi` resolve `opts.target` (defaulting to `"default"`) through `network.match()` for the `resonate:target` tag. The Rust SDK's `remote_create_req` hardcodes `func_name` as the input to `match_fn`, ignoring any user-provided target option. Users cannot override RPC targets at the context level.

- [ ] Add an optional target parameter to `Context::rpc` and `Context::begin_rpc` (or accept it via an options struct)
- [ ] Update `remote_create_req` to use the provided target (or default to `func_name`) as the input to `match_fn`
- [ ] Ensure URL targets pass through `match_fn` unchanged (this is already handled by the `match_fn` closure built in `Resonate::new`)
- [ ] Add tests: `ctx.rpc` with custom target uses that target in `resonate:target` tag, default behavior uses `func_name` through `match_fn`, URL target passes through unchanged
