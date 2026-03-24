# 🔬 Deep Codebase Audit: `resonate-sdk-rust`

**Date:** 2026-03-24
**Scope:** Full audit of all source files (~10,770 lines across 20 `.rs` files)
**Tests:** 189 passing, 0 failing, 8 clippy warnings

## Table of Contents
1. [🔴 Security](#1--security)
2. [🟠 Performance](#2--performance)
3. [🟡 Correctness](#3--correctness)
4. [🔵 Simplicity & Type Design](#4--simplicity--type-design)
5. [🟣 Code Duplication](#5--code-duplication)
6. [⚪ Dead Code & Unused Dependencies](#6--dead-code--unused-dependencies)
7. [🟤 Error Handling](#7--error-handling)
8. [🟢 API Design](#8--api-design)
9. [Summary Priority Matrix](#summary-priority-matrix)

---

## 1. 🔴 Security

### ✅ S1. `unsafe impl Sync for Context` is unnecessary and dangerous
**File:** `context.rs:59`
```rust
unsafe impl Sync for Context {}
```
All fields are already `Sync`: `String`, `AtomicU32`, `Arc<Mutex<_>>`, `Effects` (Clone/Send), `MatchFn` (Arc<dyn…+Sync>). This `unsafe` is vestigial and introduces a false sense of danger. If someone later adds a non-Sync field, the compiler won't catch it.

**Fix:** Remove it. If the compiler complains, fix the actual root cause.

---

### ✅ S2. Encryptor trait exists but is never wired into Codec
**Files:** `resonate.rs:53-57`, `codec.rs`

The `Encryptor` trait is defined, `NoopEncryptor` exists, and `ResonateConfig` accepts `encryptor: Option<Box<dyn Encryptor>>`, but **it is never passed to `Codec`**. The Codec always does plain base64. Anyone configuring an encryptor gets silent no-op.

**Impact:** Data you expect to be encrypted is stored in plaintext base64.

---

### ✅ S4. No request/response validation in `ServerState::apply`
**File:** `network.rs`

The local server state machine trusts all inputs. `promise_id`, `task_id`, etc. are extracted with `.unwrap_or("")` — empty strings become valid IDs. No validation that IDs are non-empty, that timeout values are positive, etc.

---

## 2. 🟠 Performance

### ✅ P1. `now_ms()` duplicated 5 times
**Files:** `context.rs:700`, `handle.rs:222`, `network.rs:1389`, `promises.rs:162`, `resonate.rs:836`

All 5 are identical:
```rust
fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}
```
This should be a single shared utility function.

---

### ✅ P2. Effects cache uses `tokio::sync::Mutex` instead of `DashMap`
**File:** `effects.rs:18`
```rust
cache: Arc<Mutex<HashMap<String, PromiseRecord>>>,
```
Every `create_promise` and `settle_promise` call takes an async lock on the cache — even for reads. `DashMap` is already listed in `Cargo.toml` but never used! A `DashMap` would allow lock-free concurrent reads.

---

### ✅ P4. `Codec` is a unit struct cloned everywhere (N/A — Codec now holds Encryptor state via S2 fix)
**File:** `codec.rs:14`
```rust
#[derive(Debug, Clone)]
pub struct Codec;
```
It has no fields. All methods could be free functions or the Codec could be `Copy`. The 20+ `.codec.clone()` calls are zero-cost but add visual noise. If Encryptor were properly integrated, Codec would need state — but today it doesn't.

---

### ✅ P5. `decode_promise` clones every field individually
**File:** `codec.rs:54-70`
```rust
pub fn decode_promise(&self, promise: &PromiseRecord) -> Result<PromiseRecord> {
    Ok(PromiseRecord {
        id: promise.id.clone(),
        state: promise.state.clone(),
        ...
        tags: promise.tags.clone(),
        ...
    })
}
```
This allocates a new `String` for `id`, clones the `HashMap` for `tags`, etc. — on every decode. Consider taking ownership (`fn decode_promise(self, promise: PromiseRecord) -> Result<PromiseRecord>`) when the caller doesn't need the original.

---

### C4. `Resonate::new()` spawns tasks during construction
**File:** `resonate.rs:263-403`

The constructor `spawn`s 2 background tokio tasks (subscription refresh + network start). This means:
- Construction requires an active tokio runtime
- Construction has side effects (network I/O)
- There's no way to construct without starting

This is an anti-pattern. A `start()` method would be cleaner.

---

### C5. `serde_json::to_value(args).unwrap_or(Null)` silently swallows errors
**File:** `context.rs:193, 211`
```rust
data: Some(serde_json::to_value(args).unwrap_or(serde_json::Value::Null)),
```
If serialization fails (e.g., contains non-serializable types), the data is silently replaced with `null`. This should propagate the error.

---

## 4. 🔵 Simplicity & Type Design

---

### T3. `Options` / `PartialOptions` / `OptionsBuilder` — three types for one concern
**File:** `options.rs`

`PartialOptions` is just `Options` with every field wrapped in `Option`. This is the classic "builder via duplication" pattern. A single builder struct with methods would be cleaner.

---

### T4. Two parallel request/response systems
- `send::Request` / `send::Response` — typed enums used by Core
- Raw `serde_json::json!({...})` — used by Resonate, Promises, Schedules, Handle

The `Resonate` struct bypasses the `send::Request` type system entirely, constructing JSON manually for `task.create`, `promise.create`, `promise.registerListener`, etc. This means the typed system provides no safety for the main API.

---

### T5. `Core::clone_core_ref` manually clones each field
**File:** `core.rs:61-68`
```rust
pub fn clone_core_ref(&self) -> Arc<Core> {
    Arc::new(Core {
        send: self.send.clone(),
        codec: self.codec.clone(),
        registry: self.registry.clone(),
        match_fn: self.match_fn.clone(),
        heartbeat: self.heartbeat.clone(),
    })
}
```
If Core is always behind an Arc (which it is — `self.core: Arc<Core>` in Resonate), this method is unnecessary. Just clone the `Arc<Core>`.

---

### T6. `DurableFuture` and `RemoteFuture` are structurally identical
Both have three states: `Completed(Value)`, `Failed(Value)`, `Pending(...)`. They share the same `completed()`, `failed()`, `await_result()` pattern. Only the `Pending` variant differs. These could be a single generic type parameterized on the pending source.

---

### T7. `ServerState` uses `String` for states instead of enums
**File:** `network.rs`

`DurablePromise.state: String` and `Task.state: String` use raw strings (`"pending"`, `"acquired"`, etc.) while the public API has a `PromiseState` enum. The local server should use the same enum for type safety.

---

## 5. 🟣 Code Duplication

### D1. Tag building repeated in `begin_run_by_name` and `begin_rpc`
**File:** `resonate.rs:506-511` and `resonate.rs:613-618`
```rust
// Exact same 5 lines in both methods:
tags.insert("resonate:origin".to_string(), prefixed_id.clone());
tags.insert("resonate:branch".to_string(), prefixed_id.clone());
tags.insert("resonate:parent".to_string(), prefixed_id.clone());
tags.insert("resonate:scope".to_string(), "global".to_string());
tags.insert("resonate:target".to_string(), opts.target.clone());
```
Extract to a helper: `fn build_root_tags(id: &str, target: &str, tags: &mut HashMap<...>)`.

---

### D3. `RunTask` and `RpcTask` share 80% of their structure
Both have: `child_id`, `ctx`, `req`, `record: OnceCell`, `_phantom`, plus `ensure_created()`, `id()`, `timeout()`, `IntoFuture` impl. The main differences are that `RunTask` has `func`/`args` and `RpcTask` doesn't. A shared base or generic type could eliminate ~150 lines.

---

## 7. 🟤 Error Handling

### E1. Registry panics instead of returning errors
**File:** `registry.rs:52-55`
```rust
pub fn add(&mut self, name: &str, kind: DurableKind, factory: Factory) {
    if name.is_empty() { panic!("name is required"); }
    if self.by_name.contains_key(name) {
        panic!("Function '{}' is already registered", name);
    }
}
```
And `Resonate::register` propagates the panic. Library code should not panic on invalid input. Return `Result<(), Error>`.

---

### E2. `RwLock::read().unwrap()` / `write().unwrap()` on poisoned locks
**Files:** `core.rs:185`, `resonate.rs:470, 502`
```rust
let reg = self.registry.read().unwrap();
```
If any thread panics while holding the write lock, all subsequent access panics too. Use `.read().map_err(...)` or `parking_lot::RwLock` (which doesn't poison).


---

### E4. No timeout on the `ResonateHandle::result()` busy-wait
As noted in C1, `register_listener` loops forever. There should be a configurable timeout or it should use the promise's `timeout_at` as a deadline.

---

## Summary Priority Matrix

| Priority | Category | Issues |
|----------|----------|--------|
| **Critical** | Security | S1 (unsafe Sync), S2 (encryption not wired), S3 (auth not applied) |
| **High** | Correctness | C1 (infinite busy-wait), C2 (race condition), C4 (constructor side-effects) |
| **High** | Performance | P2 (Mutex→DashMap), P3 (clone storm), P6 (linear timeout scans) |
| **Medium** | Error Handling | E1 (panics), E2 (lock poisoning), E4 (no timeout) |
| **Medium** | Simplicity | T1 (Durable trait), T4 (dual request systems), T5 (clone_core_ref) |
| **Medium** | Dead Code | All of section 6 — ~11 unused items, 1 unused dependency |
| **Low** | Duplication | D1 (tags), D2 (state matching), D3 (RunTask/RpcTask), P1 (now_ms) |
| **Low** | Polish | P4 (Codec unit struct), P8 (static tag keys), clippy warnings |
