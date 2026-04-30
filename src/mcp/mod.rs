mod tools;

use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Mutex;

type Writer = Arc<Mutex<BufWriter<tokio::io::Stdout>>>;

struct Ctx {
    server: String,
    token: Option<String>,
    session_id: String,
}

pub async fn run(server: String, token: Option<String>) {
    let session_id = gen_session_id();
    tracing::info!(session_id = %session_id, server = %server, "MCP proxy starting");

    let writer: Writer = Arc::new(Mutex::new(BufWriter::new(tokio::io::stdout())));

    {
        let sse_server = server.clone();
        let sse_token = token.clone();
        let sse_session = session_id.clone();
        let sse_writer = writer.clone();
        tokio::spawn(async move {
            sse_listener(sse_server, sse_token, sse_session, sse_writer).await;
        });
    }

    let ctx = Ctx { server, token, session_id };
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);

    loop {
        match read_message(&mut reader).await {
            Ok(Some(request)) => {
                if let Some(response) = handle(&ctx, &request).await {
                    send(&writer, response).await;
                }
            }
            Ok(None) => break,
            Err(e) => {
                tracing::error!("stdin error: {e}");
                break;
            }
        }
    }
}

async fn handle(ctx: &Ctx, req: &Value) -> Option<Value> {
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = req.get("method").and_then(|v| v.as_str()).unwrap_or("");
    let is_notification = req.as_object().map(|o| !o.contains_key("id")).unwrap_or(true);

    match method {
        "initialize" => Some(json_rpc_result(id, json!({
            "protocolVersion": "2025-11-25",
            "capabilities": {
                "tools": {},
                "experimental": { "claude/channel": {} }
            },
            "serverInfo": { "name": "resonate-mcp", "version": env!("CARGO_PKG_VERSION") }
        }))),

        "notifications/initialized" => None,

        "tools/list" => Some(json_rpc_result(id, json!({ "tools": tools_list() }))),

        "tools/call" => {
            let name = req["params"]["name"].as_str().unwrap_or("");
            let args = &req["params"]["arguments"];
            let result = call_tool(ctx, name, args).await;
            Some(json_rpc_result(id, tool_response(result)))
        }

        _ if is_notification => None,

        _ => Some(json_rpc_error(id, -32601, "Method not found")),
    }
}

async fn call_tool(ctx: &Ctx, name: &str, args: &Value) -> Result<Value, String> {
    match name {
        "promise-create" => {
            let id = args["id"].as_str().ok_or("missing id")?.to_string();
            let timeout_at = args["timeout_at"].as_i64().ok_or("missing timeout_at")?;
            let param = args.get("param").cloned();
            let tags = args.get("tags").cloned();
            tools::promise_create(&ctx.server, ctx.token.as_deref(), id, timeout_at, param, tags).await
        }
        "promise-get" => {
            let id = args["id"].as_str().ok_or("missing id")?.to_string();
            tools::promise_get(&ctx.server, ctx.token.as_deref(), id).await
        }
        "promise-settle" => {
            let id = args["id"].as_str().ok_or("missing id")?.to_string();
            let resolution = args["resolution"].as_str().ok_or("missing resolution")?.to_string();
            let value = args.get("value").cloned();
            tools::promise_settle(&ctx.server, ctx.token.as_deref(), id, resolution, value).await
        }
        "promise-search" => {
            let state_filter = args.get("state").and_then(|v| v.as_str()).map(|s| s.to_string());
            let tags = args.get("tags").cloned();
            let limit = args.get("limit").and_then(|v| v.as_i64());
            let cursor = args.get("cursor").and_then(|v| v.as_str()).map(|s| s.to_string());
            tools::promise_search(&ctx.server, ctx.token.as_deref(), state_filter, tags, limit, cursor).await
        }
        "promise-listen" => {
            let id = args["id"].as_str().ok_or("missing id")?.to_string();
            tools::promise_listen(&ctx.server, ctx.token.as_deref(), &ctx.session_id, id).await
        }
        "resonate-bash" => {
            let id = args.get("id").and_then(|v| v.as_str()).map(|s| s.to_string());
            let timeout_ms = args.get("timeoutMs").and_then(|v| v.as_u64());
            let script = args.get("script").and_then(|v| v.as_str()).map(|s| s.to_string());
            let script_path = args.get("scriptPath").and_then(|v| v.as_str()).map(|s| s.to_string());
            let bash_args = args.get("args").and_then(|v| v.as_array()).map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            });
            let tags = args.get("tags").cloned();
            tools::resonate_bash(
                &ctx.server,
                ctx.token.as_deref(),
                &ctx.session_id,
                id, timeout_ms, script, script_path, bash_args, tags,
            ).await
        }
        other => Err(format!("unknown tool '{other}'")),
    }
}

fn tools_list() -> Value {
    json!([
        {
            "name": "promise-create",
            "description": "Create a durable promise",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id":         { "type": "string",  "description": "Unique promise identifier" },
                    "timeout_at": { "type": "integer", "description": "Expiry timestamp (epoch ms)" },
                    "param":      {                    "description": "Optional input value (PromiseValue with optional headers and data fields)" },
                    "tags":       { "type": "object",  "description": "Optional key-value string tags" }
                },
                "required": ["id", "timeout_at"]
            }
        },
        {
            "name": "promise-get",
            "description": "Get a promise by ID",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Promise identifier" }
                },
                "required": ["id"]
            }
        },
        {
            "name": "promise-settle",
            "description": "Settle a promise (resolve, reject, or cancel)",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id":         { "type": "string", "description": "Promise identifier" },
                    "resolution": { "type": "string", "enum": ["resolve", "reject", "cancel"] },
                    "value":      {                   "description": "Optional result value (PromiseValue)" }
                },
                "required": ["id", "resolution"]
            }
        },
        {
            "name": "promise-search",
            "description": "Search promises with optional filters",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "state":  { "type": "string",  "description": "Filter by state" },
                    "tags":   { "type": "object",  "description": "Filter by tags" },
                    "limit":  { "type": "integer", "description": "Max results" },
                    "cursor": { "type": "string",  "description": "Pagination cursor" }
                }
            }
        },
        {
            "name": "promise-listen",
            "description": "Subscribe to push notifications when a promise settles. Registers a poll listener; the MCP server emits a notifications/message when the promise resolves, rejects, or times out.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Promise identifier to watch" }
                },
                "required": ["id"]
            }
        },
        {
            "name": "resonate-bash",
            "description": "Run a shell command as a durable, asynchronous task. Returns a promise ID immediately and registers a listener; the command executes in the background and a `<channel>` notification carrying `{exit_code, stdout, stderr}` is delivered when it finishes. The script body is stored in the promise and restarted from the top on crash, so scripts must be idempotent on restart — for 'trigger external + poll' patterns use 'check-then-trigger + poll' to avoid double-firing on restart. Ideal for fire-and-watch coordination with external systems that have a status endpoint but no webhook (Airflow runs, GitHub Actions, Terraform applies, dbt jobs, K8s rollouts) or for busy-loop polling like `until row exists in BigQuery; do sleep 30; done`. Polling consumes no LLM tokens because the loop runs in the shell, not in the model context.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id":         { "type": "string",  "description": "Promise ID (auto-generated if omitted)" },
                    "timeoutMs":  { "type": "integer", "description": "Milliseconds from now before timeout (default 300000 = 5 min)" },
                    "script":     { "type": "string",  "description": "Inline bash script (mutually exclusive with scriptPath)" },
                    "scriptPath": { "type": "string",  "description": "Named script path under bash_exec.root_dir (mutually exclusive with script)" },
                    "args":       { "type": "array",   "items": { "type": "string" }, "description": "Arguments for named script" },
                    "tags":       { "type": "object",  "description": "Additional tags merged with resonate:target" }
                }
            }
        }
    ])
}

async fn sse_listener(server: String, token: Option<String>, session_id: String, writer: Writer) {
    let url = format!("{}/poll/mcp/{}", server.trim_end_matches('/'), session_id);
    let client = reqwest::Client::new();
    loop {
        tracing::info!(url = %url, "Connecting to SSE");
        let mut req = client
            .get(&url)
            .header("Accept", "text/event-stream");
        if let Some(t) = &token {
            req = req.bearer_auth(t);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!("SSE connection established");
                if let Err(e) = drain_sse(resp, &writer).await {
                    tracing::warn!("SSE stream ended: {e}");
                }
            }
            Ok(resp) => tracing::warn!(status = %resp.status(), "SSE connection rejected"),
            Err(e)   => tracing::warn!(error = %e, "SSE connection failed"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

async fn drain_sse(mut resp: reqwest::Response, writer: &Writer) -> Result<(), String> {
    let mut buf = String::new();

    while let Some(chunk) = resp.chunk().await.map_err(|e: reqwest::Error| e.to_string())? {
        buf.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(pos) = buf.find("\n\n") {
            let event_block = buf[..pos].to_string();
            buf = buf[pos + 2..].to_string();

            for line in event_block.lines() {
                if let Some(data) = line.strip_prefix("data: ") {
                    if let Ok(payload) = serde_json::from_str::<Value>(data) {
                        let promise_obj = &payload["data"]["promise"];
                        if promise_obj.is_null() {
                            continue;
                        }
                        let handle = promise_obj["id"].as_str().unwrap_or("unknown").to_string();
                        let status = match promise_obj["state"].as_str().unwrap_or("") {
                            "resolved"          => "resolved",
                            "rejected"          => "rejected",
                            "rejected_timedout" => "timedout",
                            "rejected_canceled" => "canceled",
                            _                   => "unknown",
                        };
                        let content = serde_json::to_string(promise_obj).unwrap_or_default();
                        let notification = json!({
                            "jsonrpc": "2.0",
                            "method": "notifications/claude/channel",
                            "params": {
                                "content": content,
                                "meta": { "handle": handle, "status": status }
                            }
                        });
                        send(writer, notification).await;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn send(writer: &Writer, msg: Value) {
    let mut line = match serde_json::to_string(&msg) {
        Ok(s) => s,
        Err(e) => { tracing::error!("serialize error: {e}"); return; }
    };
    line.push('\n');
    let mut w = writer.lock().await;
    if w.write_all(line.as_bytes()).await.is_err() { return; }
    let _ = w.flush().await;
}

async fn read_message(reader: &mut BufReader<tokio::io::Stdin>) -> Result<Option<Value>, String> {
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) => return Ok(None),
            Ok(_) => {}
            Err(e) => return Err(e.to_string()),
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        return serde_json::from_str(trimmed)
            .map(Some)
            .map_err(|e| format!("JSON parse error: {e}"));
    }
}

fn tool_response(result: Result<Value, String>) -> Value {
    match result {
        Ok(v) => json!({
            "content": [{ "type": "text", "text": serde_json::to_string_pretty(&v).unwrap_or_default() }],
            "isError": false
        }),
        Err(msg) => json!({
            "content": [{ "type": "text", "text": msg }],
            "isError": true
        }),
    }
}

fn json_rpc_result(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn json_rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn gen_session_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("mcp-{}-{}", d.as_millis(), d.subsec_nanos() % 1_000_000)
}
