### P3. Hot-path clone storm in `Core::execute_until_blocked_inner`
**File:** `core.rs:220-250`

Inside the **execution loop**, every iteration allocates:
- `self.send.clone()` — Arc clone (cheap)
- `self.codec.clone()` — unit struct clone (free, but noisy)
- `root_promise.id.clone()` — **3 times** for Context + Info
- `task_data.func.clone()` — **2 times**
- `root_promise.tags.clone()` — full `HashMap` clone
- `self.match_fn.clone()` — Arc clone
- `task_data.args.clone()` — **full `serde_json::Value` deep clone**

On redirect loops, these all repeat. The `root_promise.id` could be stored as `Arc<str>` and shared. The tags clone is expensive and could use `Arc<HashMap>` or a `Cow`.

---

### P8. Tags are always `HashMap<String, String>` — allocated per promise
Tags like `"resonate:scope"`, `"resonate:target"`, `"resonate:branch"` are string-allocated on every promise creation. These well-known keys could be static `&'static str`, and the tag map could be an enum + user map, or at minimum use `Arc<str>` for keys.

---

## 3. 🟡 Correctness

### C1. `ResonateHandle::register_listener` busy-waits forever
**File:** `handle.rs:178-186`
```rust
async fn register_listener(&self) -> Result<serde_json::Value> {
    loop {
        let resp = self.register_listener_once().await?;
        if state != "pending" { return Ok(resp); }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
```
This polls every 5 seconds with no timeout, no cancellation, no backoff. If the promise is never settled, this hangs forever.

---

### C2. Race condition in `create_handle`
**File:** `resonate.rs:749-798`

The method checks `subs.get_mut(&id)` for a pre-existing notification, then inserts a new subscription entry. Between the check and insert (across an `.await` for `transport.send`), a notification could arrive and be lost — it finds no subscriber and inserts a `resolved: Some(result)` entry, but then the code overwrites it with a fresh `SubscriptionEntry { tx: Some(tx), ... }`.

---

## 6. ⚪ Dead Code & Unused Dependencies

| Item | Location | Notes |
|------|----------|-------|
| `dashmap` dependency | `Cargo.toml` | Listed but never imported |
| `Encryptor` / `NoopEncryptor` | `resonate.rs` | Trait defined, never integrated into Codec |
| `dependencies: HashMap` | `resonate.rs:115` | Set via `set_dependency()`, never read |
| `SubscriptionEntry.rx` | `resonate.rs:82` | Always `None`, `#[allow(dead_code)]` |
| `RetryPolicy` | `options.rs:65` | Defined, never used anywhere |
| `build_send()` | `send.rs:139` | HTTP client builder exists but HttpNetwork is TODO |
| `Value::headers_or_empty()` | `types.rs:38` | Never called in production code |
| `ResonateHandle::done()` | `handle.rs:154` | Public API, never called |
| `_token` / `_auth` | `resonate.rs:171-182` | Resolved from env but unused |
| `ScheduleRecord`, `TaskRecord` | `types.rs` | Defined but never deserialized into |
| `Promises::register_listener` | `promises.rs` | Public but unused |

---

### A2. Prelude re-exports `BasicAuth` and `Encryptor` — both are unused/broken
Users importing the prelude get types that don't actually work.

---

### A3. No graceful shutdown story
`Resonate::stop()` aborts background tasks. There's no drain period, no waiting for in-flight operations, no `Drop` implementation. If the Resonate struct is dropped without calling `stop()`, background tasks are leaked.

