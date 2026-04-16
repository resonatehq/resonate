# Resonate Rust SDK

[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

The Resonate Rust SDK lets you build reliable, distributed applications using Rust's async/await model.
Built on [tokio](https://tokio.rs), it gives you durable execution with automatic recovery, idempotency, and distributed coordination — without the infrastructure headache.

**Resonate** is a durable execution engine.
Write your business logic as normal async Rust functions, annotate them with `#[resonate_sdk::function]`, and Resonate handles retries, recovery, and replay.
If your process crashes mid-workflow, execution resumes from the last checkpoint — not from the beginning.

## Links

- [Evaluate Resonate](https://docs.resonatehq.io/evaluate/)
- [Example applications](https://github.com/resonatehq-examples)
- [Skill guide (Rust SDK)](https://docs.resonatehq.io/develop/rust)
- [Join the Discord](https://resonatehq.io/discord)

## Installation

```bash
cargo add resonate-sdk tokio --features tokio/full
cargo add serde --features derive
```

Or add to your `Cargo.toml` directly:

```toml
[dependencies]
resonate-sdk = "0.1"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

To track the latest development version, use a git dependency:

```toml
[dependencies]
resonate-sdk = { git = "https://github.com/resonatehq/resonate-sdk-rust", branch = "main" }
```

## Quick example

```rust
use resonate_sdk::prelude::*;
use serde::{Deserialize, Serialize};

// A workflow function — receives &Context for durable sub-task orchestration.
#[resonate_sdk::function]
async fn greet(ctx: &Context, name: String) -> Result<String> {
    let greeting = ctx.run(format_greeting, name).await?;
    Ok(greeting)
}

// A leaf function — pure computation, no Context needed.
#[resonate_sdk::function]
async fn format_greeting(name: String) -> Result<String> {
    Ok(format!("Hello, {}!", name))
}

#[tokio::main]
async fn main() {
    let resonate = Resonate::new(ResonateConfig {
        url: Some("http://localhost:8001".into()),
        ..Default::default()
    });

    resonate.register(greet).unwrap();
    resonate.register(format_greeting).unwrap();

    let result: String = resonate
        .run("greet-1", greet, "World".into())
        .await
        .expect("workflow failed");

    println!("{result}");
}
```

Run a server with `resonate dev` (install via `brew install resonatehq/tap/resonate`), then `cargo run`.

## Parallel execution

Use `.spawn()` to fan out durable tasks in parallel:

```rust
#[resonate_sdk::function]
async fn fan_out(ctx: &Context) -> Result<Vec<String>> {
    // Spawn tasks in parallel
    let h1 = ctx.run(process, "alpha".into()).spawn().await?;
    let h2 = ctx.run(process, "beta".into()).spawn().await?;
    let h3 = ctx.run(process, "gamma".into()).spawn().await?;

    // Collect results — each is individually durable
    let r1 = h1.await?;
    let r2 = h2.await?;
    let r3 = h3.await?;

    Ok(vec![r1, r2, r3])
}
```

If the process crashes after `h1` completes but before `h2` finishes, only `h2` and `h3` re-execute on recovery. `h1`'s result is replayed from the durable log.

## API overview

### Entry point

| Method | Description |
|---|---|
| `Resonate::new(config)` | Connect to a Resonate Server |
| `Resonate::local()` | In-memory mode (no server needed) |

### Client APIs (Ephemeral World)

| Method | Description |
|---|---|
| `resonate.register(func)` | Register a durable function |
| `resonate.run(id, func, args)` | Execute a function locally |
| `resonate.rpc(id, func_name, args)` | Execute a function remotely (by name) |
| `resonate.schedule(name, cron, func_name, args)` | Schedule a cron-based execution |
| `resonate.get(id)` | Get a handle to an existing execution |
| `resonate.stop()` | Graceful shutdown |
| `resonate.promises` | Sub-client for raw promise operations |
| `resonate.schedules` | Sub-client for schedule management |

### Context APIs (Durable World)

| Method | Description |
|---|---|
| `ctx.run(func, args)` | Invoke a child function locally |
| `ctx.rpc::<T>(func_name, args)` | Invoke a child function remotely |
| `ctx.sleep(duration)` | Durable sleep (survives restarts) |

All builders support `.await` (sequential) or `.spawn().await` (parallel, returns a handle).

### Builder options

All builders support:

- `.timeout(Duration)` — execution timeout
- `.target(&str)` — target worker group

Client-side builders (`resonate.run()`, `resonate.rpc()`) additionally support:

- `.version(u32)` — version tag
- `.tags(HashMap<String, String>)` — metadata tags

### Function annotation

`#[resonate_sdk::function]` detects the function kind from the first parameter:

| First parameter | Kind | Description |
|---|---|---|
| `&Context` | Workflow | Can orchestrate sub-tasks via `ctx.run()`, `ctx.rpc()`, `ctx.sleep()` |
| `&Info` | Leaf with metadata | Read-only access to execution metadata |
| *(anything else)* | Pure leaf | Stateless computation |

## Environment variables

| Variable | Description | Default |
|---|---|---|
| `RESONATE_URL` | Full server URL | *(local mode)* |
| `RESONATE_HOST` | Server hostname | *(unset)* |
| `RESONATE_PORT` | Server port | `8001` |
| `RESONATE_TOKEN` | JWT auth token | *(unset)* |
| `RESONATE_PREFIX` | Promise ID prefix | *(empty)* |

Constructor arguments take precedence over environment variables.
If no URL is configured, the SDK runs in local mode (in-memory, no server required).

## License

Apache-2.0
