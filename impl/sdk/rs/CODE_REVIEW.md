# Code Review: Resonate Rust SDK

## 🟡 MEDIUM PRIORITY

### 4. `Value::data_or_null()` always deep-clones even when ownership is available

**File:** `types.rs:33-35`, called from many sites in `context.rs`, `core.rs`

```rust
pub fn data_or_null(&self) -> serde_json::Value {
    self.data.clone().unwrap_or(serde_json::Value::Null)
}
```

**Fix:** Add `pub fn into_data_or_null(self) -> serde_json::Value` and use it on owned `Value`s.

---

### 5. `ResonateHandle::result()` deep-clones the entire watched JSON payload

**File:** `handle.rs:55`

```rust
let result = guard.as_ref().unwrap().clone();
```

**Fix:** Use `watch::Receiver<Option<Arc<PromiseResult>>>` or `PromiseResult { value: Arc<serde_json::Value> }` so `result()` clones a cheap `Arc`, not the full JSON tree.

---


### 7. `Resonate::new` creates `Transport` twice, clones config fields unnecessarily

**File:** `resonate.rs:132-250`

`config.url.clone()`, `config.pid.clone()` (twice), temporary `Transport::new(...)` in each network branch, then a final `Transport::new(network.clone())`.

**Fix:** Destructure `config` up front, build one `Transport`, derive the heartbeat sender from it.

---

### 8. `RunTask` stores args twice: typed + serialized in `PromiseCreateReq`

**File:** `context.rs:255-270`

`ctx.run(func, args)` eagerly serializes args into `PromiseCreateReq` but also keeps the original typed `args` for execution.

**Fix:** Build `PromiseCreateReq` lazily inside `ensure_created()` from `&args`.

---

### 9. `Core::on_message` boxes a future unnecessarily

**File:** `core.rs:68-74`

```rust
pub fn on_message<'a>(...) -> futures::future::BoxFuture<'a, Result<Status>> {
    Box::pin(self.on_message_inner(task_id, version))
}
```

**Fix:** Just make it `pub async fn on_message(...)`. No trait-object need here.

---

### 10. `handle.rs:93-97` allocates a `String` just to decode via `Codec`

```rust
data: Some(serde_json::Value::String(s.to_string())),
```

Creates a `Value` wrapper just to pass to `Codec::decode`.

**Fix:** Add `Codec::decode_base64_str<T>(&self, s: &str) -> Result<Option<T>>`.

---

## 🟢 LOW PRIORITY (easy wins)

### 11. `RegistryEntry` stores function name redundantly

**File:** `registry.rs:24-28, 54-59` — name is both the `HashMap` key and a field.

**Fix:** Remove `RegistryEntry.name`.

---

### 12. Local server uses `String` for state fields

**File:** `network.rs:35-73` — `DurablePromise.state` and `Task.state` are `String`.

**Fix:** Use `PromiseState`/`TaskState` enums internally, serialize at the boundary.

---

### 13. `unwrap_or_default()` hides protocol errors

**Files:** `promises.rs:33,60,85,108`, `resonate.rs:440,538,568,868`

```rust
rdata.get("promise").cloned().unwrap_or_default()
```

Turns malformed responses into `null` silently.

**Fix:** Use `.ok_or_else(|| Error::DecodingError(...))` for required fields.

---

### 14. `HttpNetwork` SSE buffer parsing is O(n²)

**File:** `http_network.rs:140-141`

```rust
let event_block = buffer[..pos].to_string();
buffer = buffer[pos + 2..].to_string();
```

Reallocates the entire tail every event.

**Fix:** Use `drain(..)` or index-based parsing. Change subscriber payloads to `Arc<str>`.

---

### 15. `PROTOCOL_VERSION` defined in 3 places

**Files:** `resonate.rs:28`, `send.rs:11`, `promises.rs:4`

**Fix:** Single definition in `lib.rs`, re-export.

---

### 16. Preallocate tag `HashMap`s

**Files:** `context.rs:167-171, 197-202, 283-288`, `resonate.rs:370-376`

Always 4–5 entries. **Fix:** `HashMap::with_capacity(5)`.

---

### 17. `Context::next_id()` uses `SeqCst` unnecessarily

**File:** `context.rs:109`

**Fix:** `Ordering::Relaxed` is sufficient for a monotonic per-context counter.

---

### 18. `version` is serialized in param but never consumed

**Files:** `resonate.rs:399-403, 509-513`, `types.rs:282-287`

`TaskData` only has `func`/`args`, so `version` is serialized but ignored on execution.

**Fix:** Either add `version` to `TaskData` or stop serializing it.

---

## Prioritization

| Priority | Finding | Impact |
|----------|---------|--------|
| ✅ | 1. Route all requests through `Sender` | Removes ~60% of raw JSON construction |
| 🔴 | 2. Factor builder state machines | Eliminates ~300 lines of repetition |
| ✅ | 3. `PromiseResult.state` → enum | Removes per-event `String` allocs |
| ✅ | 4. `Value::into_data_or_null` | Eliminates deep clones on hot path |
| ✅ | 5. `Arc<PromiseResult>` in watch | Eliminates deep clones in `handle.result()` |
| 🟡 | 6. Typed envelope structs in `send.rs` | ~6 allocs → 1 per request |
| ✅ | 7. Simplify `Resonate::new` | Fewer intermediate clones |
| 🟡 | 8. Lazy `PromiseCreateReq` in `RunTask` | Halves memory for large args |
| ✅ | 9. `Core::on_message` → plain async fn | Removes unnecessary Box allocation |
| ✅ | 10. `Codec::decode_base64_str` | Removes `String` alloc in handle decode |
| 🟢 | 11. Remove `RegistryEntry.name` | Removes redundant `String` |
| 🟢 | 12. Enums in local server state | Removes string allocs + comparisons |
| 🟢 | 13. Error on missing required fields | Correctness fix |
| 🟢 | 14. Fix SSE O(n²) buffer parsing | Perf fix for long-lived connections |
| 🟢 | 15. Single `PROTOCOL_VERSION` | DRY |
| 🟢 | 16. Preallocate tag maps | Minor perf |
| 🟢 | 17. `Relaxed` ordering | Minor perf |
| 🟢 | 18. Stop serializing unused `version` | Cleanup + smaller payloads |
