//! The tunnel: the sandbox process's stdio, carrying the Resonate protocol.
//!
//! The sandbox needs no network. It writes a request envelope as one line on
//! stdout; this reads it, applies it at the server in-process, and writes the
//! response back as one line on stdin. Newline-delimited JSON in both
//! directions, and the roles are inverted from the usual: the worker is the
//! server, the code in the sandbox is the client.
//!
//! This is a gateway, so it behaves like one — `parse_and_validate` decides
//! what the protocol admits, and a rejection is worded by `core` rather than
//! here. What it adds over the HTTP gateway is de-duplication: the follow
//! endpoint replays everything captured so far whenever the stream is
//! reopened, so a reconnect re-delivers every request the process ever wrote.
//! Replies are cached by `corrId` and replayed from the cache, which answers a
//! process still waiting on one without applying anything twice.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use resonate_core::types::{self, RequestEnvelope, ResponseEnvelope};
use resonate_core::ResonateServer;

use crate::api::{Api, Frame};

/// How a pump stopped.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The process exited. Whether it did any good is the promise's business.
    Exited,
    /// The stream could not be held open. The process may still be running.
    Lost(String),
}

/// Replies already sent, so a replayed request is answered rather than
/// re-applied.
///
/// Bounded, because a long-lived sandbox could otherwise accumulate one entry
/// per request forever. Evicting the oldest is safe: a request old enough to
/// fall out is one whose reply the process has long since read.
struct Replies {
    by_corr_id: HashMap<String, String>,
    order: VecDeque<String>,
    cap: usize,
}

impl Replies {
    fn new(cap: usize) -> Self {
        Self {
            by_corr_id: HashMap::new(),
            order: VecDeque::new(),
            cap,
        }
    }

    fn get(&self, corr_id: &str) -> Option<&String> {
        self.by_corr_id.get(corr_id)
    }

    fn put(&mut self, corr_id: String, reply: String) {
        if self.by_corr_id.insert(corr_id.clone(), reply).is_none() {
            self.order.push_back(corr_id);
            while self.order.len() > self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.by_corr_id.remove(&old);
                }
            }
        }
    }
}

/// What a line of stdout turned out to be.
enum Inbound {
    Request(RequestEnvelope),
    /// It was addressed to the protocol and the protocol will not have it.
    Rejected(ResponseEnvelope),
    /// Ordinary output. The process printed something.
    Log,
}

/// A line is a request only if `core` can salvage both a `kind` and a
/// `corrId` from it. Anything else the process prints — a log line, a stack
/// trace, structured logging that happens to have a `kind` field of its own —
/// is output, and answering it with a protocol error would be noise on the
/// process's stdin.
///
/// `salvage_context` is the same reading the HTTP gateway gives a body that
/// would not parse, so the two edges agree on what counts as addressed to the
/// protocol. Its fallbacks are the test: `("unknown", "0")` for bytes that are
/// not an envelope at all, and a request needs both halves to be answerable —
/// without a `corrId` there is nothing to correlate a rejection to.
fn classify(line: &str) -> Inbound {
    let bytes = line.as_bytes();
    match types::parse_and_validate(bytes) {
        Ok(req) => Inbound::Request(req),
        Err(invalid) => match types::salvage_context(bytes) {
            (kind, corr_id) if kind != "unknown" && corr_id != "0" => {
                Inbound::Rejected(invalid.to_response(kind, corr_id))
            }
            _ => Inbound::Log,
        },
    }
}

/// How long to wait before reopening a stream that dropped, and how often.
const RECONNECT_DELAY_MS: u64 = 500;
const RECONNECT_ATTEMPTS: u32 = 5;
const REPLY_CACHE: usize = 4096;

/// Carry the protocol between `server` and the process until it exits.
///
/// Requests are handled one at a time. That is not only simpler than serving
/// them concurrently — writes to stdin are separate HTTP requests, and two in
/// flight at once could interleave halves of two frames on the process's side.
pub async fn pump(
    server: Arc<dyn ResonateServer>,
    api: Arc<Api>,
    sandbox_id: &str,
    pid: i64,
    task_id: &str,
) -> Outcome {
    let mut replies = Replies::new(REPLY_CACHE);
    let mut attempt = 0;

    loop {
        let mut stream = match api.follow_stdout(sandbox_id, pid).await {
            Ok(s) => s,
            Err(e) => match retry(&api, sandbox_id, pid, &mut attempt, &e).await {
                Some(outcome) => return outcome,
                None => continue,
            },
        };
        attempt = 0;

        let mut trouble = None;
        while let Some(frame) = stream.next().await {
            match frame {
                Ok(Frame::Eof) => return Outcome::Exited,
                Ok(Frame::Line(line)) => {
                    if let Err(e) =
                        handle(&server, &api, sandbox_id, pid, task_id, &line, &mut replies).await
                    {
                        // Losing the write half means the process is waiting
                        // for a reply that will never come; reopening the read
                        // half would not help.
                        return Outcome::Lost(e);
                    }
                }
                Err(e) => {
                    trouble = Some(e);
                    break;
                }
            }
        }

        // The stream ended. Either the process is done — in which case the
        // `eof` above was missed only because the connection dropped first —
        // or it is still running and this is a reconnect.
        let reason = trouble.unwrap_or_else(|| "stdout stream closed".to_string());
        match retry(&api, sandbox_id, pid, &mut attempt, &reason).await {
            Some(outcome) => return outcome,
            None => continue,
        }
    }
}

/// Decide whether to reopen the stream, and wait if so.
///
/// `Some(outcome)` ends the pump: either the process is no longer running, or
/// the stream has failed too many times to keep trying.
async fn retry(
    api: &Api,
    sandbox_id: &str,
    pid: i64,
    attempt: &mut u32,
    reason: &str,
) -> Option<Outcome> {
    if let Ok(status) = api.process_status(sandbox_id, pid).await {
        if !status.running() {
            return Some(Outcome::Exited);
        }
    }
    *attempt += 1;
    if *attempt > RECONNECT_ATTEMPTS {
        return Some(Outcome::Lost(reason.to_string()));
    }
    tracing::debug!(
        sandbox_id,
        pid,
        attempt = *attempt,
        reason,
        "tensorlake: reopening the tunnel"
    );
    tokio::time::sleep(Duration::from_millis(
        RECONNECT_DELAY_MS * 2u64.pow(*attempt - 1),
    ))
    .await;
    None
}

/// Apply one line of stdout. `Err` means the reply could not be delivered.
async fn handle(
    server: &Arc<dyn ResonateServer>,
    api: &Api,
    sandbox_id: &str,
    pid: i64,
    task_id: &str,
    line: &str,
    replies: &mut Replies,
) -> Result<(), String> {
    match reply_to(server, task_id, line, replies).await? {
        Some(reply) => api.write_stdin(sandbox_id, pid, reply.into_bytes()).await,
        None => Ok(()),
    }
}

/// What one line of stdout should be answered with, if anything.
///
/// The whole protocol path, with no sandbox in it — which is what makes it
/// testable against a real server rather than only against Tensorlake.
async fn reply_to(
    server: &Arc<dyn ResonateServer>,
    task_id: &str,
    line: &str,
    replies: &mut Replies,
) -> Result<Option<String>, String> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(None);
    }

    let reply = match classify(line) {
        Inbound::Log => {
            // The sandbox's own output. Worth keeping — it is the only window
            // into a process running somewhere else.
            tracing::info!(task_id, "sandbox: {line}");
            return Ok(None);
        }
        Inbound::Rejected(resp) => {
            tracing::warn!(task_id, "tensorlake: rejected a tunnel request");
            encode(&resp)?
        }
        Inbound::Request(req) => {
            let corr_id = req.head.corr_id.clone();
            if let Some(cached) = replies.get(&corr_id) {
                // A replay: the stream was reopened and the follow endpoint
                // handed us this request a second time.
                tracing::debug!(task_id, corr_id, "tensorlake: replaying a cached reply");
                cached.clone()
            } else {
                let kind = req.kind.clone();
                let resp = match server.process(&req).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        // The server could not answer. Say so in the protocol
                        // rather than leaving the process blocked forever.
                        tracing::warn!(task_id, kind, error = %e, "tensorlake: server unavailable");
                        ResponseEnvelope::error(kind, corr_id.clone(), 503, &e.message)
                    }
                };
                let encoded = encode(&resp)?;
                replies.put(corr_id, encoded.clone());
                encoded
            }
        }
    };

    Ok(Some(reply))
}

/// One response, as one line.
fn encode(resp: &ResponseEnvelope) -> Result<String, String> {
    let mut s = serde_json::to_string(resp).map_err(|e| format!("encode response: {e}"))?;
    s.push('\n');
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn envelope(kind: &str, version: &str) -> String {
        json!({
            "kind": kind,
            "head": { "corrId": "c1", "version": version },
            "data": {},
        })
        .to_string()
    }

    #[test]
    fn a_valid_envelope_is_a_request() {
        let line = envelope("promise.get", types::PROTOCOL_VERSION);
        assert!(matches!(classify(&line), Inbound::Request(_)));
    }

    #[test]
    fn plain_output_is_a_log_line() {
        for line in [
            "hello world",
            "Traceback (most recent call last):",
            "{\"level\":\"info\",\"msg\":\"starting\"}",
            "[]",
            "null",
        ] {
            assert!(
                matches!(classify(line), Inbound::Log),
                "expected a log line: {line}"
            );
        }
    }

    #[test]
    fn an_envelope_this_build_does_not_speak_is_rejected() {
        let line = envelope("promise.get", "1999-01-01");
        match classify(&line) {
            Inbound::Rejected(resp) => {
                assert_eq!(resp.head.status, 400);
                assert_eq!(resp.head.corr_id, "c1");
            }
            _ => panic!("expected a rejection"),
        }
    }

    #[test]
    fn a_line_addressed_to_the_protocol_but_malformed_is_rejected() {
        // It has a kind and something to correlate to, so the process is
        // waiting for an answer and gets one.
        let line = json!({
            "kind": "promise.get",
            "head": { "corrId": "c1", "version": types::PROTOCOL_VERSION },
            "data": "not an object",
        })
        .to_string();
        match classify(&line) {
            Inbound::Rejected(resp) => assert_eq!(resp.head.corr_id, "c1"),
            _ => panic!("expected a rejection"),
        }
    }

    #[test]
    fn structured_logging_with_a_kind_of_its_own_is_still_output() {
        // Nothing to correlate to, so nothing is waiting on a reply.
        for line in [
            json!({ "kind": "audit", "msg": "step done" }).to_string(),
            json!({ "kind": "promise.get" }).to_string(),
        ] {
            assert!(matches!(classify(&line), Inbound::Log), "{line}");
        }
    }

    #[test]
    fn replies_are_cached_and_replayed() {
        let mut replies = Replies::new(2);
        replies.put("a".into(), "reply-a".into());
        assert_eq!(replies.get("a"), Some(&"reply-a".to_string()));
        assert_eq!(replies.get("b"), None);
    }

    #[test]
    fn the_reply_cache_evicts_the_oldest() {
        let mut replies = Replies::new(2);
        replies.put("a".into(), "1".into());
        replies.put("b".into(), "2".into());
        replies.put("c".into(), "3".into());
        assert_eq!(replies.get("a"), None);
        assert_eq!(replies.get("b"), Some(&"2".to_string()));
        assert_eq!(replies.get("c"), Some(&"3".to_string()));
    }

    #[test]
    fn re_putting_a_corr_id_does_not_grow_the_queue() {
        let mut replies = Replies::new(2);
        replies.put("a".into(), "1".into());
        replies.put("a".into(), "1".into());
        replies.put("b".into(), "2".into());
        assert_eq!(replies.get("a"), Some(&"1".to_string()));
        assert_eq!(replies.get("b"), Some(&"2".to_string()));
    }

    // ─── The protocol path, against a server ──────────────────────────────
    //
    // A stub rather than the real engine: what is under test is the tunnel —
    // that a line becomes a request, reaches `process`, and comes back as one
    // line — and the engine's own behaviour would only obscure it.

    struct StubServer {
        seen: std::sync::Mutex<Vec<String>>,
        answer: Result<i32, &'static str>,
    }

    impl StubServer {
        fn ok() -> Arc<Self> {
            Arc::new(Self {
                seen: std::sync::Mutex::new(Vec::new()),
                answer: Ok(200),
            })
        }

        fn unavailable() -> Arc<Self> {
            Arc::new(Self {
                seen: std::sync::Mutex::new(Vec::new()),
                answer: Err("the store is gone"),
            })
        }

        fn seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl ResonateServer for StubServer {
        async fn process(
            &self,
            req: &RequestEnvelope,
        ) -> Result<ResponseEnvelope, resonate_core::Unavailable> {
            self.seen.lock().unwrap().push(req.head.corr_id.clone());
            match self.answer {
                Ok(status) => Ok(ResponseEnvelope::new(
                    req.kind.clone(),
                    req.head.corr_id.clone(),
                    status,
                    json!({ "ok": true }),
                )),
                Err(e) => Err(resonate_core::Unavailable::new(e)),
            }
        }
    }

    fn request(corr_id: &str) -> String {
        json!({
            "kind": "promise.get",
            "head": { "corrId": corr_id, "version": types::PROTOCOL_VERSION },
            "data": { "id": "p1" },
        })
        .to_string()
    }

    #[tokio::test]
    async fn a_request_reaches_the_server_and_its_answer_comes_back_as_one_line() {
        let stub = StubServer::ok();
        let server: Arc<dyn ResonateServer> = stub.clone();
        let mut replies = Replies::new(8);

        let reply = reply_to(&server, "p1", &request("c1"), &mut replies)
            .await
            .unwrap()
            .expect("a request is answered");

        assert_eq!(stub.seen(), vec!["c1"]);
        assert!(reply.ends_with('\n'));
        let decoded: serde_json::Value = serde_json::from_str(reply.trim()).unwrap();
        assert_eq!(decoded["head"]["corrId"], "c1");
        assert_eq!(decoded["head"]["status"], 200);
    }

    #[tokio::test]
    async fn a_replayed_request_is_answered_from_the_cache_and_not_applied_twice() {
        let stub = StubServer::ok();
        let server: Arc<dyn ResonateServer> = stub.clone();
        let mut replies = Replies::new(8);

        // What a reconnect does: the follow endpoint hands over every line the
        // process ever wrote, this one included.
        let first = reply_to(&server, "p1", &request("c1"), &mut replies)
            .await
            .unwrap();
        let again = reply_to(&server, "p1", &request("c1"), &mut replies)
            .await
            .unwrap();

        assert_eq!(first, again, "the process gets the same answer");
        assert_eq!(stub.seen(), vec!["c1"], "and the server saw it once");
    }

    #[tokio::test]
    async fn output_the_process_printed_is_not_answered() {
        let stub = StubServer::ok();
        let server: Arc<dyn ResonateServer> = stub.clone();
        let mut replies = Replies::new(8);

        for line in ["starting up", "  ", "", "{\"msg\":\"working\"}"] {
            assert_eq!(
                reply_to(&server, "p1", line, &mut replies).await.unwrap(),
                None,
                "expected no reply to: {line}"
            );
        }
        assert!(stub.seen().is_empty(), "and nothing reached the server");
    }

    #[tokio::test]
    async fn a_server_that_cannot_answer_still_unblocks_the_process() {
        let stub = StubServer::unavailable();
        let server: Arc<dyn ResonateServer> = stub.clone();
        let mut replies = Replies::new(8);

        let reply = reply_to(&server, "p1", &request("c1"), &mut replies)
            .await
            .unwrap()
            .expect("an unanswerable request is still answered");

        let decoded: serde_json::Value = serde_json::from_str(reply.trim()).unwrap();
        assert_eq!(decoded["head"]["status"], 503);
        assert_eq!(decoded["head"]["corrId"], "c1");
    }

    #[test]
    fn a_response_is_one_line() {
        let resp = ResponseEnvelope::error("promise.get".into(), "c1".into(), 404, "nope");
        let encoded = encode(&resp).unwrap();
        assert!(encoded.ends_with('\n'));
        assert_eq!(encoded.trim().lines().count(), 1);
    }
}
