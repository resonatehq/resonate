use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use lru::LruCache;
use tokio::sync::Mutex;
use wasmtime::{Caller, Engine, Extern, Linker, Memory, Module, Store, TypedFunc};

use crate::persistence::{
    Storage, TaskAcquireParams, TaskFenceCreateParams, TaskFenceSettleParams, TaskFulfillParams,
};
use crate::server::Server;
use crate::types::PromiseState;
use crate::util::system_time_ms;

// ─── Transport ────────────────────────────────────────────────────────────────

pub struct WasmExecTransport {
    engine: Engine,
    module_cache: Arc<Mutex<LruCache<PathBuf, Arc<Module>>>>,
    root_dir: PathBuf,
    server: Arc<Server>,
}

impl WasmExecTransport {
    pub fn new(
        server: Arc<Server>,
        root_dir: impl Into<PathBuf>,
        module_cache_size: usize,
    ) -> anyhow::Result<Self> {
        let mut cfg = wasmtime::Config::new();
        cfg.async_support(true);
        let engine = Engine::new(&cfg)?;
        let cache_size = NonZeroUsize::new(module_cache_size.max(1)).unwrap();
        let module_cache = Arc::new(Mutex::new(LruCache::new(cache_size)));
        Ok(Self {
            engine,
            module_cache,
            root_dir: root_dir.into(),
            server,
        })
    }

    pub async fn send(&self, address: &str, payload: &serde_json::Value) {
        let task_id = match payload
            .pointer("/data/task/id")
            .and_then(|v| v.as_str())
        {
            Some(id) => id.to_string(),
            None => {
                tracing::warn!(address, "wasm-exec: missing task.id in execute payload");
                return;
            }
        };
        let task_version = payload
            .pointer("/data/task/version")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let module_path = match resolve_module_path(address, &self.root_dir) {
            Some(p) => p,
            None => {
                tracing::warn!(address, "wasm-exec: cannot resolve module path");
                return;
            }
        };

        let engine = self.engine.clone();
        let module_cache = Arc::clone(&self.module_cache);
        let server = Arc::clone(&self.server);
        let address = address.to_string();

        tokio::spawn(async move {
            if let Err(e) = run_task(
                engine,
                module_cache,
                server,
                task_id,
                task_version,
                module_path,
                address,
            )
            .await
            {
                tracing::error!(error = %e, "wasm-exec: task execution failed");
            }
        });
    }
}

// ─── Module loading ───────────────────────────────────────────────────────────

async fn load_module(
    engine: &Engine,
    cache: &Mutex<LruCache<PathBuf, Arc<Module>>>,
    path: &Path,
) -> anyhow::Result<Arc<Module>> {
    {
        let mut guard = cache.lock().await;
        if let Some(m) = guard.get(path) {
            return Ok(Arc::clone(m));
        }
    }

    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read wasm module: {}", path.display()))?;

    let module = Module::new(engine, &bytes)
        .with_context(|| format!("compile wasm module: {}", path.display()))?;
    let module = Arc::new(module);

    {
        let mut guard = cache.lock().await;
        guard.put(path.to_path_buf(), Arc::clone(&module));
    }

    Ok(module)
}

// ─── Task execution ───────────────────────────────────────────────────────────

async fn run_task(
    engine: Engine,
    module_cache: Arc<Mutex<LruCache<PathBuf, Arc<Module>>>>,
    server: Arc<Server>,
    task_id: String,
    task_version: i64,
    module_path: PathBuf,
    wasm_address: String,
) -> anyhow::Result<()> {
    let pid = format!("wasm-exec-{}", fastrand::u64(..));
    let lease_timeout = server.config.tasks.lease_timeout;

    // 1. Acquire — racing is normal; storage errors are transient (drop, let lease expire).
    let acquire_result = server
        .storage
        .transact({
            let task_id = task_id.clone();
            let pid = pid.clone();
            move |db| {
                db.task_acquire(&TaskAcquireParams {
                    task_id: &task_id,
                    version: task_version,
                    time: system_time_ms(),
                    ttl: lease_timeout,
                    pid: &pid,
                })
            }
        })
        .await
        .map_err(|e| anyhow!("{e}"))?;

    if !acquire_result.was_acquired {
        tracing::debug!(task_id, "wasm-exec: task not acquired");
        return Ok(());
    }

    let acquired_version = acquire_result.task_version.unwrap_or(task_version + 1);

    // Everything from here on: we hold the task. Errors fall into two buckets:
    //   permanent → reject the promise (task_fulfill rejected)
    //   transient → release the task for retry

    let promise = match acquire_result.promise {
        Some(p) => p,
        None => {
            reject_promise(&server.storage, &task_id, acquired_version, "no promise returned after acquire").await;
            return Ok(());
        }
    };
    let parent_timeout_at = promise.timeout_at;

    // 2. Decode params — malformed data is permanent.
    let param_data = match promise.param.data.as_deref() {
        Some(d) if !d.is_empty() => d.to_string(),
        _ => {
            reject_promise(&server.storage, &task_id, acquired_version, "param.data is missing").await;
            return Ok(());
        }
    };
    let (func_name, args) = match decode_param_data(&param_data) {
        Ok(v) => v,
        Err(e) => {
            reject_promise(&server.storage, &task_id, acquired_version, &format!("invalid param.data: {e}")).await;
            return Ok(());
        }
    };
    if func_name.is_empty() {
        reject_promise(&server.storage, &task_id, acquired_version, "func name is empty in param.data").await;
        return Ok(());
    }

    // 3. Load module — file-not-found is permanent; other I/O is transient.
    let module = match load_module(&engine, &module_cache, &module_path).await {
        Ok(m) => m,
        Err(e) => {
            if is_not_found_error(&e) {
                reject_promise(&server.storage, &task_id, acquired_version, &format!("module not found: {e}")).await;
            } else {
                tracing::warn!(task_id, error = %e, "wasm-exec: transient module load error, releasing");
                release_task(&server.storage, &task_id, acquired_version, lease_timeout).await;
            }
            return Ok(());
        }
    };

    // 4. Instantiate — any failure is permanent (broken module).
    let state = WasmExecState {
        storage: Arc::clone(&server.storage),
        task_id: task_id.clone(),
        task_version: acquired_version,
        #[allow(dead_code)]
        pid: pid.clone(),
        wasm_address: wasm_address.clone(),
        parent_timeout_at,
        outcome: None,
    };
    let mut store = Store::new(&engine, state);

    let linker = match build_linker(&engine) {
        Ok(l) => l,
        Err(e) => {
            reject_promise(&server.storage, &task_id, acquired_version, &format!("linker error: {e}")).await;
            return Ok(());
        }
    };
    let instance: wasmtime::Instance = match linker.instantiate_async(&mut store, &module).await {
        Ok(i) => i,
        Err(e) => {
            reject_promise(&server.storage, &task_id, acquired_version, &format!("module instantiation failed: {e}")).await;
            return Ok(());
        }
    };

    // 5. Write execute() arguments into WASM memory — failure is permanent.
    let name_ptr = match write_as_string(&mut store, &instance, &func_name).await {
        Ok(p) => p,
        Err(e) => {
            reject_promise(&server.storage, &task_id, acquired_version, &format!("failed to write func name: {e}")).await;
            return Ok(());
        }
    };
    let root_id_ptr = match write_as_string(&mut store, &instance, &task_id).await {
        Ok(p) => p,
        Err(e) => {
            reject_promise(&server.storage, &task_id, acquired_version, &format!("failed to write root id: {e}")).await;
            return Ok(());
        }
    };
    let args_ptr = match write_as_string(&mut store, &instance, &args).await {
        Ok(p) => p,
        Err(e) => {
            reject_promise(&server.storage, &task_id, acquired_version, &format!("failed to write args: {e}")).await;
            return Ok(());
        }
    };
    let execute_fn = match instance.get_typed_func::<(i32, i32, i32), i32>(&mut store, "execute") {
        Ok(f) => f,
        Err(e) => {
            reject_promise(&server.storage, &task_id, acquired_version, &format!("execute export not found: {e}")).await;
            return Ok(());
        }
    };

    // 6. Heartbeat — refreshes the lease while execute() runs.
    let heartbeat = {
        let storage = Arc::clone(&server.storage);
        let task_id = task_id.clone();
        let pid = pid.clone();
        let version = acquired_version;
        tokio::spawn(async move {
            let beat_ms = (lease_timeout / 3).max(1000) as u64;
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(beat_ms));
            interval.tick().await;
            loop {
                interval.tick().await;
                let task_id = task_id.clone();
                let pid = pid.clone();
                let now = system_time_ms();
                let _ = storage
                    .transact(move |db| db.task_heartbeat(&pid, &[(&task_id, version)], now))
                    .await;
            }
        })
    };

    // 7. Run the workflow.
    let call_result = execute_fn
        .call_async(&mut store, (name_ptr, root_id_ptr, args_ptr))
        .await;
    heartbeat.abort();

    // 8. Handle outcome.
    match call_result {
        Ok(result_ptr) => {
            if result_ptr == 0 {
                reject_promise(&server.storage, &task_id, acquired_version, "execute returned null pointer").await;
                return Ok(());
            }
            let memory = match instance.get_memory(&mut store, "memory") {
                Some(m) => m,
                None => {
                    reject_promise(&server.storage, &task_id, acquired_version, "no memory export after execute").await;
                    return Ok(());
                }
            };
            let result_value = match read_as_string(&store, memory, result_ptr) {
                Ok(s) => s,
                Err(e) => {
                    reject_promise(&server.storage, &task_id, acquired_version, &format!("failed to read result pointer: {e}")).await;
                    return Ok(());
                }
            };
            let now = system_time_ms();
            let _ = server
                .storage
                .transact({
                    let task_id = task_id.clone();
                    move |db| {
                        db.task_fulfill(&TaskFulfillParams {
                            task_id: &task_id,
                            version: acquired_version,
                            promise_id: &task_id,
                            state: "resolved",
                            value_headers: None,
                            value_data: Some(&result_value),
                            settled_at: now,
                        })
                    }
                })
                .await;
        }
        Err(_trap) => {
            match store.data_mut().outcome.take() {
                Some(ExecOutcome::Suspend(child_id)) => {
                    tracing::debug!(task_id, child_id, "wasm-exec: suspending");
                    let _ = server
                        .storage
                        .transact({
                            let task_id = task_id.clone();
                            move |db| db.task_suspend(&task_id, acquired_version, &[&child_id])
                        })
                        .await;
                }
                Some(ExecOutcome::Permanent(msg)) => {
                    tracing::warn!(task_id, msg, "wasm-exec: permanent error, rejecting promise");
                    reject_promise(&server.storage, &task_id, acquired_version, &msg).await;
                }
                Some(ExecOutcome::Transient(msg)) => {
                    tracing::warn!(task_id, msg, "wasm-exec: transient error, releasing task");
                    release_task(&server.storage, &task_id, acquired_version, lease_timeout).await;
                }
                None => {
                    tracing::error!(task_id, "wasm-exec: trap with no recorded outcome, rejecting promise");
                    reject_promise(&server.storage, &task_id, acquired_version, "wasm trap: no cause recorded").await;
                }
            }
        }
    }

    Ok(())
}

// ─── Outcome helpers ──────────────────────────────────────────────────────────

async fn reject_promise(storage: &Storage, task_id: &str, version: i64, reason: &str) {
    let now = system_time_ms();
    let task_id = task_id.to_string();
    let reason = reason.to_string();
    let _ = storage
        .transact(move |db| {
            db.task_fulfill(&TaskFulfillParams {
                task_id: &task_id,
                version,
                promise_id: &task_id,
                state: "rejected",
                value_headers: None,
                value_data: Some(&reason),
                settled_at: now,
            })
        })
        .await;
}

async fn release_task(storage: &Storage, task_id: &str, version: i64, lease_timeout: i64) {
    let now = system_time_ms();
    let task_id = task_id.to_string();
    let _ = storage
        .transact(move |db| db.task_release(&task_id, version, now, lease_timeout))
        .await;
}

fn is_not_found_error(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map_or(false, |io| io.kind() == std::io::ErrorKind::NotFound)
    })
}

// ─── Store state ──────────────────────────────────────────────────────────────

// Set by abort/create_promise/settle_promise; read after execute() returns Err.
enum ExecOutcome {
    Suspend(String),   // child promise ID — normal suspension
    Permanent(String), // error message — reject the promise
    Transient(String), // error message — release the task for retry
}

struct WasmExecState {
    storage: Arc<Storage>,
    task_id: String,
    task_version: i64,
    #[allow(dead_code)]
    pid: String,
    wasm_address: String,
    parent_timeout_at: i64,
    outcome: Option<ExecOutcome>,
}

// ─── Host imports ─────────────────────────────────────────────────────────────

fn build_linker(engine: &Engine) -> anyhow::Result<Linker<WasmExecState>> {
    let mut linker: Linker<WasmExecState> = Linker::new(engine);

    // env.abort — called by the AS runtime on errors and suspension.
    //
    // "suspend:<childId>" → normal suspension, set Suspend outcome.
    // anything else       → guest-defined error, set Permanent outcome.
    //
    // Must trap (return Err) in all cases: the AS compiler emits dead code
    // after `call abort` without an `unreachable`, relying on abort being
    // no-return. Returning normally would let execution fall through into
    // that dead code.
    linker.func_wrap(
        "env",
        "abort",
        |mut caller: Caller<'_, WasmExecState>,
         msg_ptr: i32,
         _file_ptr: i32,
         _line: i32,
         _col: i32|
         -> anyhow::Result<()> {
            let msg = caller
                .get_export("memory")
                .and_then(|e| e.into_memory())
                .and_then(|mem| read_as_string(&caller, mem, msg_ptr).ok())
                .unwrap_or_default();

            if let Some(child_id) = msg.strip_prefix("suspend:") {
                caller.data_mut().outcome = Some(ExecOutcome::Suspend(child_id.to_string()));
            } else {
                caller.data_mut().outcome =
                    Some(ExecOutcome::Permanent(format!("guest abort: {msg}")));
            }
            Err(anyhow!("wasm abort"))
        },
    )?;

    // host.create_promise — maps ctx.run/rpc to task_fence_create.
    //
    // On success returns a JSON string to the guest:
    //   {"kind":"success","value":"<v>"}  — promise already resolved
    //   {"kind":"success"}                — promise pending
    //
    // On any error, traps immediately — no value is returned to the guest.
    // Storage errors → Transient (release for retry).
    // Anything else  → Permanent (reject the promise).
    linker.func_wrap_async(
        "host",
        "create_promise",
        |mut caller: Caller<'_, WasmExecState>, (id_ptr, func_ptr, args_ptr): (i32, i32, i32)| {
            Box::new(async move {
                let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
                    Some(m) => m,
                    None => {
                        caller.data_mut().outcome = Some(ExecOutcome::Permanent(
                            "create_promise: no memory export".into(),
                        ));
                        return Err(anyhow!("no memory"));
                    }
                };

                let child_id = read_as_string(&caller, memory, id_ptr).unwrap_or_default();
                let func = read_as_string(&caller, memory, func_ptr).unwrap_or_default();
                let args = read_as_string(&caller, memory, args_ptr).unwrap_or_default();

                let storage = Arc::clone(&caller.data().storage);
                let task_id = caller.data().task_id.clone();
                let task_version = caller.data().task_version;
                let wasm_address = caller.data().wasm_address.clone();
                let parent_timeout_at = caller.data().parent_timeout_at;
                let now = system_time_ms();

                let (param_data_override, address): (Option<String>, Option<String>) =
                    if func.is_empty() {
                        (None, None)
                    } else {
                        let encoded = encode_param_data(&func, &args);
                        (Some(encoded), Some(wasm_address))
                    };

                let fence_result = storage
                    .transact(move |db| {
                        db.task_fence_create(&TaskFenceCreateParams {
                            task_id: &task_id,
                            version: task_version,
                            promise_id: &child_id,
                            state: "pending",
                            param_headers: if address.is_some() { Some("{}") } else { None },
                            param_data: param_data_override.as_deref(),
                            tags: "{}",
                            timeout_at: parent_timeout_at,
                            created_at: now,
                            settled_at: None,
                            already_timedout: false,
                            address: address.as_deref(),
                        })
                    })
                    .await;

                match fence_result {
                    Err(e) => {
                        caller.data_mut().outcome = Some(ExecOutcome::Transient(format!(
                            "create_promise: storage error: {e}"
                        )));
                        Err(anyhow!("{e}"))
                    }
                    Ok(r) if !r.fence_ok => {
                        // fence_ok=false means the task version moved — stale invocation.
                        caller.data_mut().outcome = Some(ExecOutcome::Transient(
                            "create_promise: fence_ok=false (stale invocation)".into(),
                        ));
                        Err(anyhow!("fence_ok=false"))
                    }
                    Ok(r) => match r.promise {
                        None => {
                            caller.data_mut().outcome = Some(ExecOutcome::Permanent(
                                "create_promise: no promise in fence result".into(),
                            ));
                            Err(anyhow!("no promise in fence result"))
                        }
                        Some(p) if p.state != PromiseState::Pending => {
                            let val = p.value.data.unwrap_or_default();
                            let response = serde_json::to_string(&serde_json::json!({
                                "kind": "success",
                                "value": val,
                            }))
                            .unwrap_or_else(|_| r#"{"kind":"success","value":""}"#.to_string());
                            write_as_string_in_caller(&mut caller, &response).await.map_err(|e| {
                                caller.data_mut().outcome = Some(ExecOutcome::Permanent(format!(
                                    "create_promise: failed to write response: {e}"
                                )));
                                e
                            })
                        }
                        Some(_) => {
                            write_as_string_in_caller(&mut caller, r#"{"kind":"success"}"#)
                                .await
                                .map_err(|e| {
                                    caller.data_mut().outcome = Some(ExecOutcome::Permanent(
                                        format!("create_promise: failed to write response: {e}"),
                                    ));
                                    e
                                })
                        }
                    },
                }
            })
        },
    )?;

    // host.settle_promise — maps settle_promise to task_fence_settle.
    //
    // On any error, traps immediately — no callback to the guest.
    // Storage errors → Transient. Anything else → Permanent.
    linker.func_wrap_async(
        "host",
        "settle_promise",
        |mut caller: Caller<'_, WasmExecState>, (id_ptr, value_ptr): (i32, i32)| {
            Box::new(async move {
                let memory = match caller.get_export("memory").and_then(|e| e.into_memory()) {
                    Some(m) => m,
                    None => {
                        caller.data_mut().outcome = Some(ExecOutcome::Permanent(
                            "settle_promise: no memory export".into(),
                        ));
                        return Err(anyhow!("no memory"));
                    }
                };

                let child_id = read_as_string(&caller, memory, id_ptr).unwrap_or_default();
                let value = read_as_string(&caller, memory, value_ptr).unwrap_or_default();

                let storage = Arc::clone(&caller.data().storage);
                let task_id = caller.data().task_id.clone();
                let task_version = caller.data().task_version;
                let now = system_time_ms();

                storage
                    .transact(move |db| {
                        db.task_fence_settle(&TaskFenceSettleParams {
                            task_id: &task_id,
                            version: task_version,
                            promise_id: &child_id,
                            state: "resolved",
                            value_headers: None,
                            value_data: Some(&value),
                            settled_at: now,
                        })
                    })
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        caller.data_mut().outcome = Some(ExecOutcome::Transient(format!(
                            "settle_promise: storage error: {e}"
                        )));
                        anyhow!("{e}")
                    })
            })
        },
    )?;

    Ok(linker)
}

// ─── Param encoding ───────────────────────────────────────────────────────────

// param.data on the wire is base64(JSON({func, args: [arg], version}))
// matching the resonate-sdk-ts codec format.

fn decode_param_data(data: &str) -> anyhow::Result<(String, String)> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .context("base64 decode")?;
    let json: serde_json::Value = serde_json::from_slice(&bytes).context("JSON parse")?;
    let func = json["func"].as_str().unwrap_or_default().to_string();
    let arg = json["args"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    Ok((func, arg))
}

fn encode_param_data(func: &str, arg: &str) -> String {
    use base64::Engine;
    let json = serde_json::json!({ "func": func, "args": [arg], "version": 1 });
    let s = serde_json::to_string(&json).unwrap_or_default();
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

// ─── AS string interop ────────────────────────────────────────────────────────

// AssemblyScript string layout at pointer `ptr`:
//   ptr - 4: u32 LE  — byte length of the UTF-16 data
//   ptr + 0: u16[]   — WTF-16 LE encoded code units

fn read_as_string(
    store: impl wasmtime::AsContext,
    memory: Memory,
    ptr: i32,
) -> anyhow::Result<String> {
    if ptr == 0 {
        return Ok(String::new());
    }
    let base = ptr as u32 as usize;
    if base < 4 {
        return Err(anyhow!("AS string pointer too small: {ptr}"));
    }

    let mut len_buf = [0u8; 4];
    memory
        .read(&store, base - 4, &mut len_buf)
        .context("read AS string length")?;
    let byte_len = u32::from_le_bytes(len_buf) as usize;

    if byte_len == 0 {
        return Ok(String::new());
    }
    if byte_len % 2 != 0 {
        return Err(anyhow!("AS string byte_len is odd: {byte_len}"));
    }

    let mut data = vec![0u8; byte_len];
    memory
        .read(&store, base, &mut data)
        .context("read AS string data")?;

    let utf16: Vec<u16> = data
        .chunks_exact(2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]))
        .collect();

    String::from_utf16(&utf16).context("decode UTF-16")
}

/// Allocate an AS string in a module instance (used before execute() starts).
async fn write_as_string(
    store: &mut Store<WasmExecState>,
    instance: &wasmtime::Instance,
    s: &str,
) -> anyhow::Result<i32> {
    let utf16: Vec<u16> = s.encode_utf16().collect();
    let byte_len = (utf16.len() * 2) as i32;

    let new_fn: TypedFunc<(i32, i32), i32> = instance
        .get_typed_func::<(i32, i32), i32>(&mut *store, "__new")
        .context("__new not exported — compile module with --exportRuntime")?;
    let ptr = new_fn
        .call_async(&mut *store, (byte_len, 1))
        .await
        .context("__new call")?;

    if let Ok(pin_fn) = instance.get_typed_func::<i32, i32>(&mut *store, "__pin") {
        let _ = pin_fn.call_async(&mut *store, ptr).await;
    }

    let memory = instance
        .get_memory(&mut *store, "memory")
        .ok_or_else(|| anyhow!("no memory export"))?;
    let data: Vec<u8> = utf16.iter().flat_map(|c| c.to_le_bytes()).collect();
    memory
        .write(&mut *store, ptr as u32 as usize, &data)
        .context("write AS string data")?;

    Ok(ptr)
}

/// Allocate an AS string from within a host function via Caller.
async fn write_as_string_in_caller(
    caller: &mut Caller<'_, WasmExecState>,
    s: &str,
) -> anyhow::Result<i32> {
    let utf16: Vec<u16> = s.encode_utf16().collect();
    let byte_len = (utf16.len() * 2) as i32;

    let new_fn = caller
        .get_export("__new")
        .and_then(|e| e.into_func())
        .ok_or_else(|| anyhow!("__new not exported"))?;
    let new_typed = new_fn
        .typed::<(i32, i32), i32>(&mut *caller)
        .context("__new type mismatch")?;
    let ptr = new_typed
        .call_async(&mut *caller, (byte_len, 1))
        .await
        .context("__new call")?;

    if let Some(Extern::Func(pin_fn)) = caller.get_export("__pin") {
        if let Ok(pin_typed) = pin_fn.typed::<i32, i32>(&mut *caller) {
            let _ = pin_typed.call_async(&mut *caller, ptr).await;
        }
    }

    let memory = caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .ok_or_else(|| anyhow!("no memory export"))?;
    let data: Vec<u8> = utf16.iter().flat_map(|c| c.to_le_bytes()).collect();
    memory
        .write(&mut *caller, ptr as u32 as usize, &data)
        .context("write AS string data")?;

    Ok(ptr)
}

// ─── Address helpers ──────────────────────────────────────────────────────────

/// Parse a `wasm:///relative/path.wasm` address into an absolute module path.
pub fn resolve_module_path(address: &str, root_dir: &Path) -> Option<PathBuf> {
    let parsed = url::Url::parse(address).ok()?;
    if parsed.scheme() != "wasm" {
        return None;
    }
    let path = parsed.path().trim_start_matches('/');
    if path.is_empty() {
        return None;
    }
    Some(root_dir.join(path))
}
