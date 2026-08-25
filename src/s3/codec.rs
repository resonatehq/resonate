//! The document body: format.md's snapshot encoding.
//!
//! One line of canonical JSON per entity — a header, then promises, tasks, and
//! armed deadlines — joined by `\n` with no trailing newline. This is §2–§4 of
//! `../resonate-verus/format.md`.
//!
//! What is *not* here is the append log: frames, epochs, seal and rewrite, the
//! pad, and the CAS-on-length primitive. Those need S3 Express One Zone
//! appends; v1 replaces the whole object under an ETag compare-and-swap
//! instead. The header's `v` is the dispatch point for graduating to the full
//! snapshot-plus-log layout later, which format.md names as its own evolution
//! path — so that move is a codec swap and nothing else.
//!
//! **Canonical encoding is load-bearing** (format.md §4.2). Two encoders given
//! equal state must produce identical bytes, because that is what lets the
//! shell compare bytes to decide whether a write is needed at all, and lets a
//! writer recognize its own landed write after a lost response. The rules:
//! ASCII only (everything else `\uXXXX`), minimal escapes, integers with no
//! exponent, no insignificant whitespace, fixed key order, fixed line order,
//! and omission — never `null`, `[]` or `false` — for anything empty.
//!
//! Two relaxations of §4.2, both deliberate: integers may be negative (this
//! port's clocks are `i64` milliseconds, not `nat`), and payloads are encoded
//! from the structured `PromiseValue` rather than carried as opaque bytes,
//! which is sound only because a `PromiseValue` is strings all the way down and
//! so has no float-formatting ambiguity to preserve.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde_json::Value;

use crate::core::types::{PromiseState, PromiseValue, TaskState};
use crate::kernel::state::{min_deadline, OriginDoc, PromiseDoc, TaskDoc};

/// Format version, carried in the header's `v`.
pub const DOC_FORMAT_VERSION: u64 = 1;

/// Why a document could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// Written by a newer server. Refusing is the point: a partial read would
    /// be worse than no read.
    UnsupportedVersion(u64),
    /// Not the shape a document has.
    Malformed(String),
    /// The document's `og` does not hash the key it was read from — a
    /// misrouted read, or a write that landed on the wrong key.
    OriginMismatch,
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::UnsupportedVersion(v) => write!(f, "unsupported document version {v}"),
            CodecError::Malformed(m) => write!(f, "malformed document: {m}"),
            CodecError::OriginMismatch => write!(f, "document origin does not match its key"),
        }
    }
}

impl std::error::Error for CodecError {}

/// 16 hex chars of FNV-1a over the origin string.
///
/// The origin appears in the document zero times — every id is stored relative
/// to it — but this still binds the object to its key, so a misdirected write
/// or a misrouted read is caught instead of silently corrupting another
/// workflow. FNV rather than a `Hasher` from the standard library because the
/// bytes must be identical across processes and Rust versions.
pub fn origin_hash(origin: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in origin.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{h:016x}")
}

/// An id relative to its origin: `""` for the origin's own promise, the
/// lineage after the first `':'` otherwise.
fn relative(id: &str, origin: &str) -> Option<String> {
    if id == origin {
        return Some(String::new());
    }
    id.strip_prefix(origin)
        .and_then(|rest| rest.strip_prefix(':'))
        .map(|lineage| lineage.to_string())
}

/// Inverse of [`relative`].
fn absolute(rel: &str, origin: &str) -> String {
    if rel.is_empty() {
        origin.to_string()
    } else {
        format!("{origin}:{rel}")
    }
}

fn promise_state_code(s: PromiseState) -> u8 {
    match s {
        PromiseState::Pending => 0,
        PromiseState::Resolved => 1,
        PromiseState::Rejected => 2,
        PromiseState::RejectedCanceled => 3,
        PromiseState::RejectedTimedout => 4,
    }
}

fn promise_state_of(code: i64) -> Option<PromiseState> {
    Some(match code {
        0 => PromiseState::Pending,
        1 => PromiseState::Resolved,
        2 => PromiseState::Rejected,
        3 => PromiseState::RejectedCanceled,
        4 => PromiseState::RejectedTimedout,
        _ => return None,
    })
}

fn task_state_code(s: TaskState) -> u8 {
    match s {
        TaskState::Pending => 0,
        TaskState::Acquired => 1,
        TaskState::Suspended => 2,
        TaskState::Halted => 3,
        TaskState::Fulfilled => 4,
    }
}

fn task_state_of(code: i64) -> Option<TaskState> {
    Some(match code {
        0 => TaskState::Pending,
        1 => TaskState::Acquired,
        2 => TaskState::Suspended,
        3 => TaskState::Halted,
        4 => TaskState::Fulfilled,
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Canonical writing
// ---------------------------------------------------------------------------

/// Append `s` as a quoted JSON string, ASCII-only.
///
/// Non-ASCII is `\uXXXX` with lowercase hex, supplementary planes as surrogate
/// pairs. `/` is never escaped. This is the rule that makes byte equality
/// achievable across implementations.
fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c if (c as u32) < 0x7f => out.push(c),
            c => {
                let cp = c as u32;
                if cp <= 0xffff {
                    let _ = write!(out, "\\u{cp:04x}");
                } else {
                    let v = cp - 0x1_0000;
                    let hi = 0xd800 + (v >> 10);
                    let lo = 0xdc00 + (v & 0x3ff);
                    let _ = write!(out, "\\u{hi:04x}\\u{lo:04x}");
                }
            }
        }
    }
    out.push('"');
}

/// `"key":` — a field name and its colon.
fn write_key(out: &mut String, key: &str, first: &mut bool) {
    if *first {
        *first = false;
    } else {
        out.push(',');
    }
    write_string(out, key);
    out.push(':');
}

fn write_int(out: &mut String, v: i64) {
    let _ = write!(out, "{v}");
}

/// A string map, keys sorted by code unit — the one place data supplies keys.
fn write_map(out: &mut String, map: &BTreeMap<String, String>) {
    out.push('{');
    let mut first = true;
    for (k, v) in map {
        write_key(out, k, &mut first);
        write_string(out, v);
    }
    out.push('}');
}

/// A payload: `{"h":{..},"d":".."}`, each half omitted when absent.
fn write_payload(out: &mut String, v: &PromiseValue) {
    out.push('{');
    let mut first = true;
    if let Some(headers) = &v.headers {
        write_key(out, "h", &mut first);
        let sorted: BTreeMap<String, String> =
            headers.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        write_map(out, &sorted);
    }
    if let Some(data) = &v.data {
        write_key(out, "d", &mut first);
        write_string(out, data);
    }
    out.push('}');
}

fn payload_is_empty(v: &PromiseValue) -> bool {
    v.headers.is_none() && v.data.is_none()
}

fn write_strings(out: &mut String, items: &[String]) {
    out.push('[');
    for (i, s) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_string(out, s);
    }
    out.push(']');
}

// ---------------------------------------------------------------------------
// encode
// ---------------------------------------------------------------------------

/// Encode a document for the object at `origin`'s key.
///
/// The origin is a parameter, not a field of the document: it is the key, and
/// the body stores every id relative to it.
pub fn encode(doc: &OriginDoc, origin: &str) -> Vec<u8> {
    let mut lines: Vec<String> = Vec::with_capacity(1 + doc.promises.len() + doc.tasks.len());

    // Header.
    let mut h = String::new();
    let mut first = true;
    h.push('{');
    write_key(&mut h, "t", &mut first);
    write_string(&mut h, "h");
    write_key(&mut h, "v", &mut first);
    write_int(&mut h, DOC_FORMAT_VERSION as i64);
    write_key(&mut h, "clk", &mut first);
    write_int(&mut h, doc.clock);
    write_key(&mut h, "g", &mut first);
    write_int(&mut h, doc.gen as i64);
    write_key(&mut h, "og", &mut first);
    write_string(&mut h, &origin_hash(origin));
    if let Some(ta) = doc.timer_at {
        write_key(&mut h, "ta", &mut first);
        write_int(&mut h, ta);
    }
    h.push('}');
    lines.push(h);

    // Promises, by id. Sorting full ids and sorting relative ids agree: every
    // id in the document shares the origin prefix.
    for (id, p) in &doc.promises {
        let rel = relative(id, origin).unwrap_or_else(|| id.clone());
        let mut l = String::new();
        let mut first = true;
        l.push('{');
        write_key(&mut l, "t", &mut first);
        write_string(&mut l, "p");
        write_key(&mut l, "id", &mut first);
        write_string(&mut l, &rel);
        write_key(&mut l, "st", &mut first);
        write_int(&mut l, promise_state_code(p.state) as i64);
        if !p.tags.is_empty() {
            write_key(&mut l, "tg", &mut first);
            write_map(&mut l, &p.tags);
        }
        if !payload_is_empty(&p.param) {
            write_key(&mut l, "pm", &mut first);
            write_payload(&mut l, &p.param);
        }
        if !payload_is_empty(&p.value) {
            write_key(&mut l, "vl", &mut first);
            write_payload(&mut l, &p.value);
        }
        write_key(&mut l, "to", &mut first);
        write_int(&mut l, p.timeout_at);
        write_key(&mut l, "ca", &mut first);
        write_int(&mut l, p.created_at);
        if let Some(sa) = p.settled_at {
            write_key(&mut l, "sa", &mut first);
            write_int(&mut l, sa);
        }
        if !p.callbacks.is_empty() {
            write_key(&mut l, "cb", &mut first);
            let rels: Vec<String> = p
                .callbacks
                .iter()
                .map(|a| relative(a, origin).unwrap_or_else(|| a.clone()))
                .collect();
            write_strings(&mut l, &rels);
        }
        if !p.listeners.is_empty() {
            write_key(&mut l, "ls", &mut first);
            write_strings(&mut l, &p.listeners);
        }
        l.push('}');
        lines.push(l);
    }

    // Tasks, by id.
    for (id, t) in &doc.tasks {
        let rel = relative(id, origin).unwrap_or_else(|| id.clone());
        let mut l = String::new();
        let mut first = true;
        l.push('{');
        write_key(&mut l, "t", &mut first);
        write_string(&mut l, "k");
        write_key(&mut l, "id", &mut first);
        write_string(&mut l, &rel);
        write_key(&mut l, "st", &mut first);
        write_int(&mut l, task_state_code(t.state) as i64);
        write_key(&mut l, "v", &mut first);
        write_int(&mut l, t.version);
        if let Some(pid) = &t.pid {
            write_key(&mut l, "pid", &mut first);
            write_string(&mut l, pid);
        }
        if let Some(ttl) = t.ttl {
            write_key(&mut l, "ttl", &mut first);
            write_int(&mut l, ttl);
        }
        if !t.resumes.is_empty() {
            write_key(&mut l, "rs", &mut first);
            let rels: Vec<String> = t
                .resumes
                .iter()
                .map(|a| relative(a, origin).unwrap_or_else(|| a.clone()))
                .collect();
            write_strings(&mut l, &rels);
        }
        l.push('}');
        lines.push(l);
    }

    // Armed deadlines, sorted by (deadline, id) so that the first line of each
    // kind *is* the minimum — the reading property format.md §4 keeps.
    let mut pt: Vec<(i64, String)> = doc
        .promises
        .iter()
        .filter(|(_, p)| p.timeout_armed())
        .map(|(id, p)| {
            (
                p.timeout_at,
                relative(id, origin).unwrap_or_else(|| id.clone()),
            )
        })
        .collect();
    pt.sort();
    for (dl, rel) in pt {
        let mut l = String::new();
        let mut first = true;
        l.push('{');
        write_key(&mut l, "t", &mut first);
        write_string(&mut l, "pt");
        write_key(&mut l, "dl", &mut first);
        write_int(&mut l, dl);
        write_key(&mut l, "id", &mut first);
        write_string(&mut l, &rel);
        l.push('}');
        lines.push(l);
    }

    let mut kt: Vec<(i64, String, u8)> = Vec::new();
    for (id, t) in &doc.tasks {
        let rel = relative(id, origin).unwrap_or_else(|| id.clone());
        if let Some(dl) = t.retry_at {
            kt.push((dl, rel.clone(), 0));
        }
        if let Some(dl) = t.lease_at {
            kt.push((dl, rel, 1));
        }
    }
    kt.sort();
    for (dl, rel, kind) in kt {
        let mut l = String::new();
        let mut first = true;
        l.push('{');
        write_key(&mut l, "t", &mut first);
        write_string(&mut l, "kt");
        write_key(&mut l, "dl", &mut first);
        write_int(&mut l, dl);
        write_key(&mut l, "id", &mut first);
        write_string(&mut l, &rel);
        write_key(&mut l, "k", &mut first);
        write_int(&mut l, kind as i64);
        l.push('}');
        lines.push(l);
    }

    lines.join("\n").into_bytes()
}

// ---------------------------------------------------------------------------
// decode
// ---------------------------------------------------------------------------

fn field<'a>(line: &'a Value, key: &str) -> Result<&'a Value, CodecError> {
    line.get(key)
        .ok_or_else(|| CodecError::Malformed(format!("missing field {key}")))
}

fn int_field(line: &Value, key: &str) -> Result<i64, CodecError> {
    field(line, key)?
        .as_i64()
        .ok_or_else(|| CodecError::Malformed(format!("field {key} is not an integer")))
}

fn str_field(line: &Value, key: &str) -> Result<String, CodecError> {
    Ok(field(line, key)?
        .as_str()
        .ok_or_else(|| CodecError::Malformed(format!("field {key} is not a string")))?
        .to_string())
}

fn string_map(v: &Value) -> Result<BTreeMap<String, String>, CodecError> {
    let obj = v
        .as_object()
        .ok_or_else(|| CodecError::Malformed("expected an object".into()))?;
    let mut out = BTreeMap::new();
    for (k, v) in obj {
        let s = v
            .as_str()
            .ok_or_else(|| CodecError::Malformed(format!("value of {k} is not a string")))?;
        out.insert(k.clone(), s.to_string());
    }
    Ok(out)
}

fn payload(v: &Value) -> Result<PromiseValue, CodecError> {
    let headers = match v.get("h") {
        Some(h) => Some(
            string_map(h)?
                .into_iter()
                .collect::<std::collections::HashMap<_, _>>(),
        ),
        None => None,
    };
    let data = match v.get("d") {
        Some(d) => Some(
            d.as_str()
                .ok_or_else(|| CodecError::Malformed("payload d is not a string".into()))?
                .to_string(),
        ),
        None => None,
    };
    Ok(PromiseValue { headers, data })
}

/// An array of strings, read as written.
fn verbatim_list(v: &Value) -> Result<Vec<String>, CodecError> {
    let arr = v
        .as_array()
        .ok_or_else(|| CodecError::Malformed("expected an array".into()))?;
    arr.iter()
        .map(|v| {
            v.as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| CodecError::Malformed("array element is not a string".into()))
        })
        .collect()
}

/// An array of origin-relative ids, read back as absolute ones.
fn id_list(v: &Value, origin: &str) -> Result<Vec<String>, CodecError> {
    Ok(verbatim_list(v)?
        .into_iter()
        .map(|rel| absolute(&rel, origin))
        .collect())
}

/// Read a document from the object at `origin`'s key.
///
/// Unknown line types and unknown fields are skipped: that, plus
/// omission-when-empty, is the whole evolution story — a newer server may add
/// both without breaking an older reader.
pub fn decode(bytes: &[u8], origin: &str) -> Result<OriginDoc, CodecError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| CodecError::Malformed(format!("not utf-8: {e}")))?;
    let mut doc = OriginDoc::default();
    let mut saw_header = false;
    // Armed task deadlines arrive on their own lines, after the task lines.
    let mut retry: BTreeMap<String, i64> = BTreeMap::new();
    let mut lease: BTreeMap<String, i64> = BTreeMap::new();

    for raw in text.split('\n') {
        if raw.is_empty() {
            // The pad, in a future append layout. Nothing to read.
            continue;
        }
        let line: Value = serde_json::from_str(raw)
            .map_err(|e| CodecError::Malformed(format!("bad line: {e}")))?;
        let kind = line.get("t").and_then(|v| v.as_str()).unwrap_or("");
        match kind {
            "h" => {
                let v = int_field(&line, "v")?;
                if v != DOC_FORMAT_VERSION as i64 {
                    return Err(CodecError::UnsupportedVersion(v.max(0) as u64));
                }
                if str_field(&line, "og")? != origin_hash(origin) {
                    return Err(CodecError::OriginMismatch);
                }
                doc.clock = int_field(&line, "clk")?;
                doc.gen = int_field(&line, "g")?.max(0) as u64;
                doc.timer_at = line.get("ta").and_then(|v| v.as_i64());
                saw_header = true;
            }
            "p" => {
                let id = absolute(&str_field(&line, "id")?, origin);
                let state = promise_state_of(int_field(&line, "st")?)
                    .ok_or_else(|| CodecError::Malformed("unknown promise state".into()))?;
                doc.promises.insert(
                    id,
                    PromiseDoc {
                        state,
                        param: match line.get("pm") {
                            Some(v) => payload(v)?,
                            None => PromiseValue::default(),
                        },
                        value: match line.get("vl") {
                            Some(v) => payload(v)?,
                            None => PromiseValue::default(),
                        },
                        tags: match line.get("tg") {
                            Some(v) => string_map(v)?,
                            None => BTreeMap::new(),
                        },
                        timeout_at: int_field(&line, "to")?,
                        created_at: int_field(&line, "ca")?,
                        settled_at: line.get("sa").and_then(|v| v.as_i64()),
                        // `cb` holds ids, so it is origin-relative; `ls` holds
                        // addresses, which are not ids and are stored verbatim.
                        callbacks: match line.get("cb") {
                            Some(v) => id_list(v, origin)?,
                            None => Vec::new(),
                        },
                        listeners: match line.get("ls") {
                            Some(v) => verbatim_list(v)?,
                            None => Vec::new(),
                        },
                    },
                );
            }
            "k" => {
                let id = absolute(&str_field(&line, "id")?, origin);
                let state = task_state_of(int_field(&line, "st")?)
                    .ok_or_else(|| CodecError::Malformed("unknown task state".into()))?;
                doc.tasks.insert(
                    id,
                    TaskDoc {
                        state,
                        version: int_field(&line, "v")?,
                        pid: line
                            .get("pid")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                        ttl: line.get("ttl").and_then(|v| v.as_i64()),
                        resumes: match line.get("rs") {
                            Some(v) => id_list(v, origin)?.into_iter().collect(),
                            None => BTreeSet::new(),
                        },
                        retry_at: None,
                        lease_at: None,
                    },
                );
            }
            "kt" => {
                let id = absolute(&str_field(&line, "id")?, origin);
                let dl = int_field(&line, "dl")?;
                match int_field(&line, "k")? {
                    0 => {
                        retry.insert(id, dl);
                    }
                    1 => {
                        lease.insert(id, dl);
                    }
                    other => {
                        return Err(CodecError::Malformed(format!("unknown timer kind {other}")))
                    }
                }
            }
            // `pt` lines are the armed promise deadlines, which are implied by
            // a promise being pending and targeted; they exist so a reader can
            // find the minimum without decoding the whole document.
            "pt" => {}
            _ => {}
        }
    }

    if !saw_header {
        return Err(CodecError::Malformed("no header line".into()));
    }
    for (id, dl) in retry {
        if let Some(t) = doc.tasks.get_mut(&id) {
            t.retry_at = Some(dl);
        }
    }
    for (id, dl) in lease {
        if let Some(t) = doc.tasks.get_mut(&id) {
            t.lease_at = Some(dl);
        }
    }
    debug_assert_eq!(
        doc.timer_at,
        min_deadline(&doc),
        "decoded header's timer disagrees with the decoded state"
    );
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::state::{apply_effects, KernelCfg, OriginDoc, Req};
    use crate::kernel::{drain, handle};
    use serde_json::json;

    const W: &str = "http://worker:9999";
    const ORIGIN: &str = "diff";

    fn cfg() -> KernelCfg {
        KernelCfg {
            retry_timeout: 30_000,
        }
    }

    fn apply(doc: &OriginDoc, v: serde_json::Value, kind: &str, now: i64) -> OriginDoc {
        let req = match kind {
            "create" => Req::PromiseCreate(serde_json::from_value(v).unwrap()),
            "settle" => Req::PromiseSettle(serde_json::from_value(v).unwrap()),
            "listener" => Req::PromiseRegisterListener(serde_json::from_value(v).unwrap()),
            "callback" => Req::PromiseRegisterCallback(serde_json::from_value(v).unwrap()),
            "task.create" => Req::TaskCreate(serde_json::from_value(v).unwrap()),
            other => panic!("unknown fixture kind {other}"),
        };
        let (fx, reply) = handle(doc, &req, now, &cfg());
        assert!(reply.status < 400, "fixture request failed: {reply:?}");
        let mut next = doc.clone();
        apply_effects(&mut next, &fx);
        next
    }

    /// A document exercising every line type: a settled promise with a value, a
    /// pending targeted promise with a retry timer, an acquired task with a
    /// lease, registrations, and payloads.
    fn rich() -> OriginDoc {
        let mut doc = OriginDoc::default();
        doc = apply(
            &doc,
            json!({ "id": "diff:a", "timeoutAt": 100_000,
                    "param": { "headers": { "x": "1" }, "data": "aGk=" },
                    "tags": { "resonate:target": W, "resonate:branch": "diff" } }),
            "create",
            1_000,
        );
        doc = apply(
            &doc,
            json!({ "id": "diff:b", "timeoutAt": 200_000, "param": {}, "tags": {} }),
            "create",
            1_000,
        );
        doc = apply(
            &doc,
            json!({ "id": "diff", "timeoutAt": 300_000, "param": {},
                    "tags": { "resonate:target": W } }),
            "create",
            1_000,
        );
        doc = apply(
            &doc,
            json!({ "awaited": "diff:b", "address": "poll://any@group" }),
            "listener",
            1_100,
        );
        doc = apply(
            &doc,
            json!({ "awaited": "diff:b", "address": "http://other" }),
            "listener",
            1_100,
        );
        doc = apply(
            &doc,
            json!({ "awaited": "diff:b", "awaiter": "diff:a" }),
            "callback",
            1_200,
        );
        doc = apply(
            &doc,
            json!({ "pid": "p1", "ttl": 5_000,
                    "action": { "kind": "promise.create", "head": {}, "data": {
                        "id": "diff:t", "timeoutAt": 400_000, "param": {},
                        "tags": { "resonate:target": W } } } }),
            "task.create",
            1_300,
        );
        // One settled promise, with a value and a settled_at stamp.
        doc = apply(
            &doc,
            json!({ "id": "diff:b", "state": "resolved", "value": { "data": "b2s=" } }),
            "settle",
            1_400,
        );
        doc.clock = 1_400;
        doc.gen = 7;
        doc
    }

    #[test]
    fn a_document_round_trips() {
        let doc = rich();
        let bytes = encode(&doc, ORIGIN);
        assert_eq!(decode(&bytes, ORIGIN).expect("decodes"), doc);
    }

    #[test]
    fn an_empty_document_round_trips() {
        let doc = OriginDoc::default();
        let bytes = encode(&doc, ORIGIN);
        assert_eq!(String::from_utf8(bytes.clone()).unwrap().lines().count(), 1);
        assert_eq!(decode(&bytes, ORIGIN).expect("decodes"), doc);
    }

    #[test]
    fn a_drained_document_round_trips() {
        let doc = rich();
        let fx = drain(&doc, 500_000, &cfg());
        let mut next = doc.clone();
        apply_effects(&mut next, &fx);
        let bytes = encode(&next, ORIGIN);
        assert_eq!(decode(&bytes, ORIGIN).expect("decodes"), next);
    }

    #[test]
    fn encoding_is_a_function_of_the_state_alone() {
        // Two documents built by different routes to the same state must encode
        // to the same bytes — that is what lets the shell compare bytes to
        // decide whether a write is needed.
        let a = rich();
        let mut b = decode(&encode(&a, ORIGIN), ORIGIN).unwrap();
        assert_eq!(encode(&a, ORIGIN), encode(&b, ORIGIN));
        b.gen += 1;
        assert_ne!(encode(&a, ORIGIN), encode(&b, ORIGIN));
    }

    #[test]
    fn the_header_is_the_first_line_and_names_the_version_and_origin() {
        let doc = rich();
        let text = String::from_utf8(encode(&doc, ORIGIN)).unwrap();
        let header: Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        assert_eq!(header["t"], "h");
        assert_eq!(header["v"], 1);
        assert_eq!(header["clk"], 1_400);
        assert_eq!(header["g"], 7);
        assert_eq!(header["og"], origin_hash(ORIGIN));
        assert_eq!(header["ta"], doc.timer_at.unwrap());
    }

    #[test]
    fn a_disarmed_document_omits_the_headers_timer() {
        let text = String::from_utf8(encode(&OriginDoc::default(), ORIGIN)).unwrap();
        assert!(!text.contains("\"ta\""));
    }

    #[test]
    fn ids_are_stored_relative_to_the_origin() {
        let doc = rich();
        let text = String::from_utf8(encode(&doc, ORIGIN)).unwrap();
        // No id in the body carries the origin: the root promise's is "" and
        // every other one is the lineage after the colon. (Tags are user data
        // and are stored verbatim, origin-looking values included.)
        assert!(
            !text.contains(r#""id":"diff"#),
            "an id kept its origin prefix"
        );
        assert!(text.contains(r#""t":"p","id":"","st""#), "root promise");
        assert!(text.contains(r#""t":"p","id":"a""#));
    }

    #[test]
    fn lines_come_in_a_fixed_order() {
        let doc = rich();
        let text = String::from_utf8(encode(&doc, ORIGIN)).unwrap();
        let kinds: Vec<String> = text
            .lines()
            .map(|l| {
                serde_json::from_str::<Value>(l).unwrap()["t"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        let first_of = |k: &str| kinds.iter().position(|x| x == k);
        assert_eq!(first_of("h"), Some(0));
        assert!(first_of("p") < first_of("k"));
        assert!(first_of("k") < first_of("pt"));
        assert!(first_of("pt") < first_of("kt"));
    }

    #[test]
    fn the_first_timeout_line_of_each_kind_is_the_minimum() {
        let doc = rich();
        let text = String::from_utf8(encode(&doc, ORIGIN)).unwrap();
        for kind in ["pt", "kt"] {
            let deadlines: Vec<i64> = text
                .lines()
                .filter_map(|l| {
                    let v: Value = serde_json::from_str(l).unwrap();
                    (v["t"] == kind).then(|| v["dl"].as_i64().unwrap())
                })
                .collect();
            assert!(!deadlines.is_empty(), "{kind} lines exist");
            assert_eq!(
                deadlines[0],
                *deadlines.iter().min().unwrap(),
                "{kind} lines are sorted by deadline"
            );
        }
    }

    #[test]
    fn a_task_timeout_line_names_which_timer_it_is() {
        let doc = rich();
        let text = String::from_utf8(encode(&doc, ORIGIN)).unwrap();
        let kinds: Vec<(String, i64)> = text
            .lines()
            .filter_map(|l| {
                let v: Value = serde_json::from_str(l).unwrap();
                (v["t"] == "kt").then(|| {
                    (
                        v["id"].as_str().unwrap().to_string(),
                        v["k"].as_i64().unwrap(),
                    )
                })
            })
            .collect();
        // diff:t was claimed by task.create, so it holds a lease (k = 1); the
        // rest are pending with retry timers (k = 0).
        assert!(kinds.contains(&("t".to_string(), 1)));
        assert!(kinds.contains(&("a".to_string(), 0)));
    }

    #[test]
    fn registration_order_survives_the_round_trip() {
        let doc = rich();
        let back = decode(&encode(&doc, ORIGIN), ORIGIN).unwrap();
        // Listener order is protocol-visible, so it must not be normalized.
        assert_eq!(
            back.promises["diff:b"].listeners,
            doc.promises["diff:b"].listeners
        );
    }

    #[test]
    fn listener_addresses_are_not_treated_as_ids() {
        let doc = rich();
        let back = decode(&encode(&doc, ORIGIN), ORIGIN).unwrap();
        // A settled promise's listeners were drained, so use a pending one.
        let text = String::from_utf8(encode(&doc, ORIGIN)).unwrap();
        assert!(!text.contains("diff:poll://"), "address was origin-qualified");
        assert_eq!(back, doc);
    }

    #[test]
    fn empty_fields_are_omitted_not_nulled() {
        let mut doc = OriginDoc::default();
        doc = apply(
            &doc,
            json!({ "id": "diff:x", "timeoutAt": 100, "param": {}, "tags": {} }),
            "create",
            0,
        );
        let text = String::from_utf8(encode(&doc, ORIGIN)).unwrap();
        assert!(!text.contains("null"));
        for absent in ["\"tg\"", "\"pm\"", "\"vl\"", "\"sa\"", "\"cb\"", "\"ls\""] {
            assert!(!text.contains(absent), "{absent} should be omitted");
        }
    }

    #[test]
    fn non_ascii_is_escaped() {
        let mut doc = OriginDoc::default();
        doc = apply(
            &doc,
            json!({ "id": "diff:x", "timeoutAt": 100, "param": {},
                    "tags": { "k": "café \u{1f600}\n\"q\"" } }),
            "create",
            0,
        );
        let bytes = encode(&doc, ORIGIN);
        assert!(bytes.iter().all(|b| b.is_ascii()), "body must be ASCII");
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(text.contains(r"caf\u00e9"));
        assert!(text.contains(r"\ud83d\ude00"), "surrogate pair");
        assert!(text.contains(r"\n"), "newline is escaped");
        assert!(text.contains(r#"\"q\""#), "quote is escaped");
        assert_eq!(decode(&bytes, ORIGIN).unwrap(), doc);
    }

    #[test]
    fn tag_keys_are_sorted() {
        let mut doc = OriginDoc::default();
        doc = apply(
            &doc,
            json!({ "id": "diff:x", "timeoutAt": 100, "param": {},
                    "tags": { "z": "1", "a": "2", "m": "3" } }),
            "create",
            0,
        );
        let text = String::from_utf8(encode(&doc, ORIGIN)).unwrap();
        assert!(text.contains(r#""tg":{"a":"2","m":"3","z":"1"}"#));
    }

    #[test]
    fn a_document_read_from_the_wrong_key_is_rejected() {
        let doc = rich();
        let bytes = encode(&doc, ORIGIN);
        assert_eq!(decode(&bytes, "other"), Err(CodecError::OriginMismatch));
    }

    #[test]
    fn a_newer_version_is_refused_rather_than_half_read() {
        let bytes = br#"{"t":"h","v":99,"clk":0,"g":0,"og":"x"}"#;
        assert_eq!(
            decode(bytes, ORIGIN),
            Err(CodecError::UnsupportedVersion(99))
        );
    }

    #[test]
    fn a_body_without_a_header_is_malformed() {
        let bytes = br#"{"t":"p","id":"a","st":0,"to":1,"ca":0}"#;
        assert!(matches!(
            decode(bytes, ORIGIN),
            Err(CodecError::Malformed(_))
        ));
    }

    #[test]
    fn unknown_line_types_and_fields_are_skipped() {
        let doc = rich();
        let mut text = String::from_utf8(encode(&doc, ORIGIN)).unwrap();
        // A line type this reader has never heard of, and blank pad lines.
        text.push_str("\n{\"t\":\"f\",\"seq\":1,\"anything\":[1,2,3]}\n\n\n");
        let back = decode(text.as_bytes(), ORIGIN).expect("skips what it does not know");
        assert_eq!(back, doc);
    }

    #[test]
    fn a_negative_clock_round_trips() {
        // format.md restricts integers to non-negative; this port's clocks are
        // i64 milliseconds, so the encoder carries a sign.
        let doc = OriginDoc {
            clock: -5,
            ..Default::default()
        };
        assert_eq!(decode(&encode(&doc, ORIGIN), ORIGIN).unwrap(), doc);
    }

    #[test]
    fn the_origin_hash_is_stable() {
        // Pinned: the hash is written into every document, so changing it
        // invalidates every stored object.
        assert_eq!(origin_hash(""), "cbf29ce484222325");
        assert_eq!(origin_hash("diff").len(), 16);
        assert_ne!(origin_hash("a"), origin_hash("b"));
    }
}
