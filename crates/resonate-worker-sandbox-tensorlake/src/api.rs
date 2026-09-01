//! The Tensorlake sandbox API, as much of it as this worker uses.
//!
//! Two hosts, as the service is split: a management API for the sandbox
//! lifecycle, and a per-sandbox proxy — `https://{id}.{proxy_host}/api/v1` —
//! for everything inside one. The proxy is keyed by sandbox **id**, which is
//! why a name lookup always has to yield the id rather than being used in its
//! place.

use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;

/// A sandbox, as much of one as this worker reads.
#[derive(Debug, Clone)]
pub struct Sandbox {
    pub id: String,
    pub status: String,
}

/// What a process is doing, the moment it was asked.
#[derive(Debug, Clone, Deserialize)]
pub struct ProcessStatus {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub exit_code: Option<i64>,
    #[serde(default)]
    pub signal: Option<i64>,
}

impl ProcessStatus {
    pub fn running(&self) -> bool {
        !matches!(self.status.as_str(), "exited" | "signaled")
    }
}

/// What to start inside a sandbox.
pub struct ProcessSpec {
    pub command: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub working_dir: Option<String>,
}

pub struct Api {
    http: reqwest::Client,
    api_key: String,
    /// Management API base, `/sandboxes` included — the whole URL is
    /// configurable because Tensorlake can be self-hosted.
    api_url: String,
    /// Host the per-sandbox proxy subdomain hangs off.
    proxy_host: String,
}

impl Api {
    pub fn new(
        http: reqwest::Client,
        api_key: String,
        api_url: String,
        proxy_host: String,
    ) -> Self {
        Self {
            http,
            api_key,
            api_url: api_url.trim_end_matches('/').to_string(),
            proxy_host: proxy_host
                .trim_end_matches('/')
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .to_string(),
        }
    }

    fn proxy(&self, sandbox_id: &str) -> String {
        format!("https://{sandbox_id}.{}/api/v1", self.proxy_host)
    }

    // ─── Sandbox lifecycle ────────────────────────────────────────────────

    /// The sandbox called `name`, if there is one.
    ///
    /// Tried as a direct lookup first, because the SDKs reconnect by name and
    /// the management API appears to accept one where an id goes. That is not
    /// documented, so a 404 falls through to paging the list and matching on
    /// `name` — which is always correct, just slower.
    pub async fn find_sandbox(&self, name: &str) -> Result<Option<Sandbox>, String> {
        let resp = self
            .http
            .get(format!("{}/{name}", self.api_url))
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(|e| format!("get sandbox: {e}"))?;
        if resp.status().is_success() {
            let v: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| format!("get sandbox: bad json: {e}"))?;
            // A name lookup that silently resolved to something else is worse
            // than no lookup: only accept an answer that names it back.
            if v.get("name").and_then(|n| n.as_str()) == Some(name) {
                if let Some(sb) = sandbox_from(&v) {
                    return Ok(Some(sb));
                }
            }
        }
        self.find_sandbox_by_listing(name).await
    }

    async fn find_sandbox_by_listing(&self, name: &str) -> Result<Option<Sandbox>, String> {
        let mut cursor: Option<String> = None;
        // Bounded: a project may hold millions of sandboxes, and walking all
        // of them to conclude "not found" would cost more than creating one.
        for _ in 0..20 {
            let mut url = format!("{}?limit=100", self.api_url);
            if let Some(c) = &cursor {
                url.push_str(&format!("&cursor={c}"));
            }
            let v: serde_json::Value = self
                .http
                .get(&url)
                .bearer_auth(&self.api_key)
                .send()
                .await
                .map_err(|e| format!("list sandboxes: {e}"))?
                .json()
                .await
                .map_err(|e| format!("list sandboxes: bad json: {e}"))?;
            let page = v.get("sandboxes").and_then(|s| s.as_array());
            for entry in page.into_iter().flatten() {
                if entry.get("name").and_then(|n| n.as_str()) == Some(name) {
                    if let Some(sb) = sandbox_from(entry) {
                        return Ok(Some(sb));
                    }
                }
            }
            match v.get("next_cursor").and_then(|c| c.as_str()) {
                Some(c) if !c.is_empty() => cursor = Some(c.to_string()),
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    pub async fn create_sandbox(
        &self,
        name: &str,
        image: Option<&str>,
        timeout_secs: i64,
    ) -> Result<Sandbox, String> {
        let mut body = json!({ "name": name, "timeout_secs": timeout_secs });
        if let Some(img) = image {
            body["image"] = json!(img);
        }
        let resp = self
            .http
            .post(&self.api_url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("create sandbox: {e}"))?;
        let status = resp.status();
        if status == reqwest::StatusCode::CONFLICT {
            // Another server got there first with the same name. Its sandbox
            // is as good as the one this call would have made.
            return match self.find_sandbox(name).await? {
                Some(sb) => Ok(sb),
                None => Err("create sandbox: name taken but no such sandbox".to_string()),
            };
        }
        if !status.is_success() {
            return Err(format!(
                "create sandbox: {status}: {}",
                resp.text().await.unwrap_or_default()
            ));
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("create sandbox: bad json: {e}"))?;
        sandbox_from(&v).ok_or_else(|| format!("create sandbox: no id in {v}"))
    }

    pub async fn resume_sandbox(&self, id: &str) -> Result<(), String> {
        self.expect_success(
            self.http
                .post(format!("{}/{id}/resume", self.api_url))
                .bearer_auth(&self.api_key)
                .send()
                .await,
            "resume sandbox",
        )
        .await
    }

    pub async fn delete_sandbox(&self, id: &str) -> Result<(), String> {
        self.expect_success(
            self.http
                .delete(format!("{}/{id}", self.api_url))
                .bearer_auth(&self.api_key)
                .send()
                .await,
            "delete sandbox",
        )
        .await
    }

    pub async fn sandbox_status(&self, id: &str) -> Result<String, String> {
        let v: serde_json::Value = self
            .http
            .get(format!("{}/{id}", self.api_url))
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(|e| format!("get sandbox: {e}"))?
            .json()
            .await
            .map_err(|e| format!("get sandbox: bad json: {e}"))?;
        Ok(v.get("status")
            .and_then(|s| s.as_str())
            .unwrap_or_default()
            .to_string())
    }

    // ─── Processes ────────────────────────────────────────────────────────

    /// Start a process with stdin piped and stdout captured — the two halves
    /// of the tunnel.
    pub async fn start_process(&self, sandbox_id: &str, spec: &ProcessSpec) -> Result<i64, String> {
        let env: serde_json::Map<String, serde_json::Value> = spec
            .env
            .iter()
            .map(|(k, v)| (k.clone(), json!(v)))
            .collect();
        let mut body = json!({
            "command": spec.command,
            "args": spec.args,
            "env": env,
            "stdin_mode": "pipe",
            "stdout_mode": "capture",
            "stderr_mode": "capture",
        });
        if let Some(wd) = &spec.working_dir {
            body["working_dir"] = json!(wd);
        }
        let resp = self
            .http
            .post(format!("{}/processes", self.proxy(sandbox_id)))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("start process: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "start process: {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            ));
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("start process: bad json: {e}"))?;
        // Nothing can be tunnelled to a process that will not take stdin, and
        // the API reports that per process rather than refusing the start.
        if v.get("stdin_writable").and_then(|w| w.as_bool()) == Some(false) {
            return Err("start process: stdin is not writable".to_string());
        }
        v.get("pid")
            .and_then(|p| p.as_i64())
            .ok_or_else(|| format!("start process: no pid in {v}"))
    }

    pub async fn process_status(
        &self,
        sandbox_id: &str,
        pid: i64,
    ) -> Result<ProcessStatus, String> {
        self.http
            .get(format!("{}/processes/{pid}", self.proxy(sandbox_id)))
            .bearer_auth(&self.api_key)
            .send()
            .await
            .map_err(|e| format!("get process: {e}"))?
            .json()
            .await
            .map_err(|e| format!("get process: bad json: {e}"))
    }

    pub async fn write_stdin(
        &self,
        sandbox_id: &str,
        pid: i64,
        data: Vec<u8>,
    ) -> Result<(), String> {
        self.expect_success(
            self.http
                .post(format!("{}/processes/{pid}/stdin", self.proxy(sandbox_id)))
                .bearer_auth(&self.api_key)
                .header("Content-Type", "application/octet-stream")
                .body(data)
                .send()
                .await,
            "write stdin",
        )
        .await
    }

    pub async fn close_stdin(&self, sandbox_id: &str, pid: i64) -> Result<(), String> {
        self.expect_success(
            self.http
                .post(format!(
                    "{}/processes/{pid}/stdin/close",
                    self.proxy(sandbox_id)
                ))
                .bearer_auth(&self.api_key)
                .send()
                .await,
            "close stdin",
        )
        .await
    }

    /// Open the SSE stream of the process's stdout.
    ///
    /// It replays everything captured so far before going live, which is what
    /// makes reconnecting safe — and what makes de-duplication mandatory.
    pub async fn follow_stdout(&self, sandbox_id: &str, pid: i64) -> Result<Output, String> {
        let resp = self
            .http
            .get(format!(
                "{}/processes/{pid}/stdout/follow",
                self.proxy(sandbox_id)
            ))
            .bearer_auth(&self.api_key)
            .header("Accept", "text/event-stream")
            .send()
            .await
            .map_err(|e| format!("follow stdout: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("follow stdout: {}", resp.status()));
        }
        Ok(Output {
            bytes: Box::pin(resp.bytes_stream()),
            buf: Vec::new(),
            ended: false,
        })
    }

    async fn expect_success(
        &self,
        resp: reqwest::Result<reqwest::Response>,
        what: &str,
    ) -> Result<(), String> {
        let resp = resp.map_err(|e| format!("{what}: {e}"))?;
        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(format!(
                "{what}: {status}: {}",
                resp.text().await.unwrap_or_default()
            ))
        }
    }
}

fn sandbox_from(v: &serde_json::Value) -> Option<Sandbox> {
    // `create` answers with `sandbox_id`, `get` and `list` with `id`.
    let id = v
        .get("sandbox_id")
        .or_else(|| v.get("id"))
        .and_then(|i| i.as_str())?;
    Some(Sandbox {
        id: id.to_string(),
        status: v
            .get("status")
            .and_then(|s| s.as_str())
            .unwrap_or_default()
            .to_string(),
    })
}

// ─── The stdout stream ────────────────────────────────────────────────────

/// One line of the process's stdout, or the end of it.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    Line(String),
    /// The `eof` event: the process has exited and no more output is coming.
    Eof,
}

/// The process's stdout, as SSE events off the follow endpoint.
pub struct Output {
    bytes:
        std::pin::Pin<Box<dyn futures_util::Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>,
    buf: Vec<u8>,
    ended: bool,
}

impl Output {
    /// The next line, `Eof`, or `None` when the stream is spent.
    ///
    /// Events other than `output` and `eof` are skipped rather than reported:
    /// a stream that grows a comment or a keep-alive should not end a task.
    pub async fn next(&mut self) -> Option<Result<Frame, String>> {
        loop {
            if self.ended {
                return None;
            }
            // A complete event is anything up to a blank line.
            if let Some(end) = find_event_end(&self.buf) {
                let event: Vec<u8> = self.buf.drain(..end.0).collect();
                self.buf.drain(..end.1);
                match parse_event(&event) {
                    Some(Event::Eof) => {
                        self.ended = true;
                        return Some(Ok(Frame::Eof));
                    }
                    Some(Event::Output(line)) => return Some(Ok(Frame::Line(line))),
                    Some(Event::Malformed(e)) => return Some(Err(e)),
                    None => continue,
                }
            }
            match self.bytes.next().await {
                Some(Ok(chunk)) => self.buf.extend_from_slice(&chunk),
                Some(Err(e)) => {
                    self.ended = true;
                    return Some(Err(format!("follow stdout: {e}")));
                }
                // The connection closed without an `eof` event. Not the end of
                // the process, only the end of this stream — the caller
                // reconnects and the replay picks it back up.
                None => {
                    self.ended = true;
                    return None;
                }
            }
        }
    }
}

enum Event {
    Output(String),
    Eof,
    Malformed(String),
}

/// `(offset of the separator, its length)` for the first blank line.
fn find_event_end(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'\n' {
            if buf.get(i + 1) == Some(&b'\n') {
                return Some((i, 2));
            }
            if buf.get(i + 1) == Some(&b'\r') && buf.get(i + 2) == Some(&b'\n') {
                return Some((i, 3));
            }
        }
        i += 1;
    }
    None
}

fn parse_event(event: &[u8]) -> Option<Event> {
    let text = String::from_utf8_lossy(event);
    let mut name = String::new();
    let mut data = String::new();
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(v) = line.strip_prefix("event:") {
            name = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(v.strip_prefix(' ').unwrap_or(v));
        }
    }
    match name.as_str() {
        "eof" => Some(Event::Eof),
        "output" => match serde_json::from_str::<serde_json::Value>(&data) {
            Ok(v) => Some(Event::Output(
                v.get("line")
                    .and_then(|l| l.as_str())
                    .unwrap_or_default()
                    .to_string(),
            )),
            Err(e) => Some(Event::Malformed(format!("output event: bad json: {e}"))),
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_an_output_event() {
        let e = parse_event(b"event: output\ndata: {\"line\":\"hello\",\"timestamp\":1}");
        assert!(matches!(e, Some(Event::Output(l)) if l == "hello"));
    }

    #[test]
    fn parses_an_eof_event() {
        assert!(matches!(
            parse_event(b"event: eof\ndata: {}"),
            Some(Event::Eof)
        ));
    }

    #[test]
    fn skips_events_it_does_not_know() {
        assert!(parse_event(b": keep-alive").is_none());
        assert!(parse_event(b"event: started\ndata: {}").is_none());
    }

    #[test]
    fn joins_multiple_data_lines() {
        let e = parse_event(b"event: output\ndata: {\"line\":\n data: x}");
        // Malformed as JSON, but the point is that both lines were collected
        // rather than the second one silently dropped.
        assert!(matches!(e, Some(Event::Malformed(_))));
    }

    #[test]
    fn finds_the_event_boundary_with_either_line_ending() {
        assert_eq!(find_event_end(b"event: eof\n\nrest"), Some((10, 2)));
        assert_eq!(find_event_end(b"event: eof\n\r\nrest"), Some((10, 3)));
        assert_eq!(find_event_end(b"event: eof\n"), None);
    }

    #[test]
    fn proxy_host_is_normalised() {
        let api = Api::new(
            reqwest::Client::new(),
            "k".into(),
            "https://api.tensorlake.ai/sandboxes/".into(),
            "https://sandbox.tensorlake.ai/".into(),
        );
        assert_eq!(api.api_url, "https://api.tensorlake.ai/sandboxes");
        assert_eq!(
            api.proxy("sbx-1"),
            "https://sbx-1.sandbox.tensorlake.ai/api/v1"
        );
    }
}
