
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

### T1. `Durable` trait uses `Option<&Context>` + `Option<&Info>` — should be an enum
**File:** `durable.rs:27-31`
```rust
fn execute(&self, ctx: Option<&Context>, info: Option<&Info>, args: Args) -> ...;
```
In practice: workflows always get `(Some(ctx), None)`, leaves get `(None, Some(info))` or `(None, None)`. This is a **boolean blindness** anti-pattern. Better:
```rust
enum ExecutionContext<'a> {
    Workflow(&'a Context),
    Leaf(Option<&'a Info>),
}
```

---

### T2. `SendFn` type alias is extremely complex
**File:** `send.rs:133-134`
```rust
pub type SendFn = Arc<
    dyn Fn(Request) -> Pin<Box<dyn Future<Output = Result<Response>> + Send>>
    + Send + Sync
>;
```
This could be an `async_trait`-based trait, which would be far more readable and allow `impl Send` bounds naturally.

---

### D2. Promise state matching repeated ~6 times
The pattern:
```rust
match state {
    "resolved" => { /* decode and return Ok */ }
    "rejected" | "rejected_canceled" | "rejected_timedout" => { /* decode and return Err */ }
}
```
appears in `handle.rs` (3x), `context.rs` (`IntoFuture` for `RunTask`, `IntoFuture` for `RpcTask`, `spawn` for `RunTask`, `spawn` for `RpcTask`), `core.rs`, `futures.rs`. Extract to a shared function.

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

### E3. Error type doesn't implement `Clone`
**File:** `error.rs`

`Error` wraps `reqwest::Error`, `serde_json::Error`, etc. which don't implement `Clone`. This forces the lossy `outcome_to_sendable` conversion. Consider using `Arc<Error>` for shared errors, or making Error clonable via string snapshots.

## 8. 🟢 API Design

### A1. `Resonate::run` takes `_func: D` for name only
**File:** `resonate.rs:435`
```rust
pub async fn run<D, Args, T>(
    &self, id: &str, _func: D, args: Args, opts: Option<PartialOptions>,
) -> Result<T>
where D: Durable<Args, T>, ...
{
    self.run_by_name::<T>(id, D::NAME, json_args, opts).await
}
```
The function value is passed only to extract `D::NAME` at compile time. This forces users to construct (or pass) the struct. A cleaner API would be `run::<MyFunc>(id, args, opts)` using turbofish, though this requires a different trait design.

---

### A2. Prelude re-exports `BasicAuth` and `Encryptor` — both are unused/broken
Users importing the prelude get types that don't actually work.

---

### A3. No graceful shutdown story
`Resonate::stop()` aborts background tasks. There's no drain period, no waiting for in-flight operations, no `Drop` implementation. If the Resonate struct is dropped without calling `stop()`, background tasks are leaked.

---

### A4. `Context` doesn't implement `Clone` or `Send` explicitly
The `unsafe impl Sync` hack is needed because Context is shared across async task boundaries. A redesign where Context operations go through a channel to a single owner would be safer and eliminate the unsafe.

---
