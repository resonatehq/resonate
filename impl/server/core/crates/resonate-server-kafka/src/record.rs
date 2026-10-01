//! What a record on the log holds: one promise with its task, or one schedule.
//!
//! # Contract
//!
//! - **One record per promise**, keyed by the promise id. A task's id *is* its
//!   promise's id and a promise has at most one task, so the pair is one value.
//!   Every record is the object's whole current version, never a delta: that is
//!   what makes log compaction keep a complete state, and what makes replaying
//!   a record twice harmless.
//! - The value is the blob backend's document encoding of a document that holds
//!   exactly that promise and its task — the header, the `p` line, the `k` line
//!   and the armed-deadline lines. The codec is reused as it is, so the
//!   canonical form, the format version and the origin check all carry over,
//!   and the deadlines a record arms travel inside it.
//! - A deleted promise is a tombstone: a record with no value.
//! - [`diff`] is the whole write path: the records a transition from one origin
//!   document to another needs, and nothing for what did not change — each in
//!   both encodings below.
//!
//! # Two encodings
//!
//! The **log** holds the format above: durable, read by every node and every
//! future version, so versioned and evolvable. The **local store** holds the
//! same promise and task as [postcard](https://docs.rs/postcard) ([`local`]):
//! a compact binary that decodes three to four times faster
//! (`examples/local_codec.rs`). The local format needs no evolution story of
//! its own — the store is a disposable copy, stamped with
//! [`crate::local::LOCAL_FORMAT`] and rebuilt from the log whenever the stamp
//! is not this build's. Schedules are few and never on the hot path, so they
//! keep the log's encoding in both places.
//!
//! # Dependencies
//!
//! The blob crate's kernel types and codec, and its schedule document.
//!
//! # Dependants
//!
//! The partition shell encodes with [`diff`] before every commit, transcodes
//! with [`local::from_log`] on replay, and decodes with [`local::assemble`] on
//! every load; search and the snapshot decode local records too.

use std::collections::BTreeSet;

use resonate_server_blob::codec;
use resonate_server_blob::kernel::state::{min_deadline, OriginDoc, PromiseDoc, TaskDoc};
use resonate_server_blob::schedules::{ScheduleDoc, SCHEDULE_FORMAT_VERSION};

use crate::keys::origin_of;

/// Encode promise `id` and its task as one record value.
pub fn encode_promise(id: &str, promise: &PromiseDoc, task: Option<&TaskDoc>) -> Vec<u8> {
    let mut doc = OriginDoc::default();
    doc.promises.insert(id.to_string(), promise.clone());
    if let Some(task) = task {
        doc.tasks.insert(id.to_string(), task.clone());
    }
    // The header's timer must be what the lines arm; the decoder checks.
    doc.timer_at = min_deadline(&doc);
    codec::encode(&doc, origin_of(id))
}

/// Decode the record value of promise `id`.
pub fn decode_promise(id: &str, bytes: &[u8]) -> Result<(PromiseDoc, Option<TaskDoc>), String> {
    let mut doc = codec::decode(bytes, origin_of(id)).map_err(|e| format!("record {id}: {e}"))?;
    let promise = doc
        .promises
        .remove(id)
        .ok_or_else(|| format!("record {id}: holds no promise {id}"))?;
    if !doc.promises.is_empty() {
        return Err(format!("record {id}: holds more than one promise"));
    }
    let task = doc.tasks.remove(id);
    if !doc.tasks.is_empty() {
        return Err(format!("record {id}: holds a task of another promise"));
    }
    Ok((promise, task))
}

/// Assemble an origin's document from its promise records.
///
/// The clock and generation are diagnostic in the blob document and absent
/// here; the armed deadline is derived, as the kernel derives it.
pub fn assemble<'a>(
    records: impl IntoIterator<Item = (String, &'a [u8])>,
) -> Result<OriginDoc, String> {
    let mut doc = OriginDoc::default();
    for (id, bytes) in records {
        let (promise, task) = decode_promise(&id, bytes)?;
        if let Some(task) = task {
            doc.tasks.insert(id.clone(), task);
        }
        doc.promises.insert(id, promise);
    }
    doc.timer_at = min_deadline(&doc);
    Ok(doc)
}

/// One promise's change: its new value for the log and for the local store,
/// or `None` in both for a tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub id: String,
    pub log: Option<Vec<u8>>,
    pub local: Option<Vec<u8>>,
}

/// The records that take an origin from `before` to `after`: a value for every
/// promise whose promise or task changed, a tombstone for every promise that
/// is gone. Unchanged promises produce nothing.
pub fn diff(before: &OriginDoc, after: &OriginDoc) -> Vec<Change> {
    let ids: BTreeSet<&String> = before
        .promises
        .keys()
        .chain(after.promises.keys())
        .collect();
    let mut out = Vec::new();
    for id in ids {
        let old = (before.promises.get(id), before.tasks.get(id));
        let new = (after.promises.get(id), after.tasks.get(id));
        if old == new {
            continue;
        }
        out.push(match new.0 {
            Some(promise) => Change {
                id: id.clone(),
                log: Some(encode_promise(id, promise, new.1)),
                local: Some(local::encode(promise, new.1)),
            },
            None => Change {
                id: id.clone(),
                log: None,
                local: None,
            },
        });
    }
    out
}

/// Encode a schedule as a record value.
pub fn encode_schedule(doc: &ScheduleDoc) -> Vec<u8> {
    serde_json::to_vec(doc).expect("a schedule serializes")
}

/// Decode a schedule record value.
pub fn decode_schedule(bytes: &[u8]) -> Result<ScheduleDoc, String> {
    let doc: ScheduleDoc = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    if doc.v != SCHEDULE_FORMAT_VERSION {
        return Err(format!("unsupported schedule version {}", doc.v));
    }
    Ok(doc)
}

/// The local store's encoding of a promise and its task.
pub mod local {
    use std::collections::{BTreeMap, BTreeSet};

    use resonate_core::types::{PromiseState, PromiseValue, TaskState};
    use resonate_server_blob::kernel::state::{min_deadline, OriginDoc, PromiseDoc, TaskDoc};
    use serde::{Deserialize, Serialize};

    // The kernel's types carry no serde derives, and postcard is not
    // self-describing — every field is always written, in order — so the
    // shape is spelled out here. Changing it means bumping LOCAL_FORMAT.

    #[derive(Serialize, Deserialize)]
    struct Promise {
        state: u8,
        param: Value,
        value: Value,
        tags: Vec<(String, String)>,
        timeout_at: i64,
        created_at: i64,
        settled_at: Option<i64>,
        callbacks: Vec<String>,
        listeners: Vec<String>,
        task: Option<Task>,
    }

    #[derive(Serialize, Deserialize)]
    struct Value {
        headers: Option<Vec<(String, String)>>,
        data: Option<String>,
    }

    #[derive(Serialize, Deserialize)]
    struct Task {
        state: u8,
        version: i64,
        pid: Option<String>,
        ttl: Option<i64>,
        resumes: Vec<String>,
        retry_at: Option<i64>,
        lease_at: Option<i64>,
    }

    const PROMISE_STATES: [PromiseState; 5] = [
        PromiseState::Pending,
        PromiseState::Resolved,
        PromiseState::Rejected,
        PromiseState::RejectedCanceled,
        PromiseState::RejectedTimedout,
    ];
    const TASK_STATES: [TaskState; 5] = [
        TaskState::Pending,
        TaskState::Acquired,
        TaskState::Suspended,
        TaskState::Halted,
        TaskState::Fulfilled,
    ];

    fn code<T: PartialEq>(all: &[T], s: &T) -> u8 {
        all.iter()
            .position(|x| x == s)
            .expect("every state has a code") as u8
    }

    fn state<T: Copy>(all: &[T], c: u8) -> Result<T, String> {
        all.get(c as usize)
            .copied()
            .ok_or_else(|| format!("unknown state code {c}"))
    }

    fn value_of(v: &PromiseValue) -> Value {
        Value {
            headers: v.headers.as_ref().map(|h| {
                let mut h: Vec<(String, String)> =
                    h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                h.sort();
                h
            }),
            data: v.data.clone(),
        }
    }

    fn value_from(v: Value) -> PromiseValue {
        PromiseValue {
            headers: v.headers.map(|h| h.into_iter().collect()),
            data: v.data,
        }
    }

    /// Encode a promise and its task for the local store.
    pub fn encode(p: &PromiseDoc, t: Option<&TaskDoc>) -> Vec<u8> {
        let shape = Promise {
            state: code(&PROMISE_STATES, &p.state),
            param: value_of(&p.param),
            value: value_of(&p.value),
            tags: p.tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            timeout_at: p.timeout_at,
            created_at: p.created_at,
            settled_at: p.settled_at,
            callbacks: p.callbacks.clone(),
            listeners: p.listeners.clone(),
            task: t.map(|t| Task {
                state: code(&TASK_STATES, &t.state),
                version: t.version,
                pid: t.pid.clone(),
                ttl: t.ttl,
                resumes: t.resumes.iter().cloned().collect(),
                retry_at: t.retry_at,
                lease_at: t.lease_at,
            }),
        };
        postcard::to_allocvec(&shape).expect("a promise encodes")
    }

    /// Decode a local value back into a promise and its task.
    pub fn decode(bytes: &[u8]) -> Result<(PromiseDoc, Option<TaskDoc>), String> {
        let shape: Promise = postcard::from_bytes(bytes).map_err(|e| e.to_string())?;
        let task = match shape.task {
            Some(t) => Some(TaskDoc {
                state: state(&TASK_STATES, t.state)?,
                version: t.version,
                pid: t.pid,
                ttl: t.ttl,
                resumes: t.resumes.into_iter().collect::<BTreeSet<_>>(),
                retry_at: t.retry_at,
                lease_at: t.lease_at,
            }),
            None => None,
        };
        Ok((
            PromiseDoc {
                state: state(&PROMISE_STATES, shape.state)?,
                param: value_from(shape.param),
                value: value_from(shape.value),
                tags: shape.tags.into_iter().collect::<BTreeMap<_, _>>(),
                timeout_at: shape.timeout_at,
                created_at: shape.created_at,
                settled_at: shape.settled_at,
                callbacks: shape.callbacks,
                listeners: shape.listeners,
            },
            task,
        ))
    }

    /// Transcode a log record of promise `id` into its local value — what a
    /// replay writes.
    pub fn from_log(id: &str, bytes: &[u8]) -> Result<Vec<u8>, String> {
        let (p, t) = super::decode_promise(id, bytes)?;
        Ok(encode(&p, t.as_ref()))
    }

    /// Assemble an origin's document from its local values.
    pub fn assemble<'a>(
        records: impl IntoIterator<Item = (String, &'a [u8])>,
    ) -> Result<OriginDoc, String> {
        let mut doc = OriginDoc::default();
        for (id, bytes) in records {
            let (promise, task) = decode(bytes).map_err(|e| format!("record {id}: {e}"))?;
            if let Some(task) = task {
                doc.tasks.insert(id.clone(), task);
            }
            doc.promises.insert(id, promise);
        }
        doc.timer_at = min_deadline(&doc);
        Ok(doc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use resonate_core::types::{PromiseState, PromiseValue, TaskState};
    use std::collections::{BTreeMap, BTreeSet};

    fn promise(state: PromiseState, targeted: bool) -> PromiseDoc {
        let mut tags = BTreeMap::new();
        if targeted {
            tags.insert("resonate:target".to_string(), "http://w:1".to_string());
        }
        PromiseDoc {
            state,
            param: PromiseValue {
                headers: None,
                data: Some("aGk=".into()),
            },
            value: PromiseValue::default(),
            tags,
            timeout_at: 900_000,
            created_at: 1_000,
            settled_at: (state != PromiseState::Pending).then_some(2_000),
            callbacks: Vec::new(),
            listeners: vec!["poll://any@g".into()],
        }
    }

    fn task(state: TaskState) -> TaskDoc {
        TaskDoc {
            state,
            version: 3,
            pid: Some("p1".into()),
            ttl: Some(60_000),
            resumes: BTreeSet::new(),
            retry_at: None,
            lease_at: Some(61_000),
        }
    }

    #[test]
    fn a_promise_and_its_task_round_trip() {
        let p = promise(PromiseState::Pending, true);
        let t = task(TaskState::Acquired);
        let bytes = encode_promise("o:charge", &p, Some(&t));
        assert_eq!(decode_promise("o:charge", &bytes).unwrap(), (p, Some(t)));
    }

    #[test]
    fn the_local_encoding_round_trips_and_agrees_with_the_log() {
        let mut p = promise(PromiseState::Pending, true);
        p.param.headers = Some([("a".to_string(), "1".to_string())].into());
        p.callbacks = vec!["o:x".into()];
        let mut t = task(TaskState::Acquired);
        t.resumes.insert("o:y".into());
        for (p, t) in [
            (p.clone(), Some(t)),
            (promise(PromiseState::Resolved, false), None),
        ] {
            let bytes = local::encode(&p, t.as_ref());
            assert_eq!(local::decode(&bytes).unwrap(), (p.clone(), t.clone()));
            // Replay's transcode lands on the same bytes a commit writes.
            let log = encode_promise("o:charge", &p, t.as_ref());
            assert_eq!(local::from_log("o:charge", &log).unwrap(), bytes);
        }
        assert!(local::decode(b"garbage").is_err());
    }

    #[test]
    fn a_promise_without_a_task_round_trips() {
        let p = promise(PromiseState::Resolved, false);
        let bytes = encode_promise("o", &p, None);
        assert_eq!(decode_promise("o", &bytes).unwrap(), (p, None));
    }

    #[test]
    fn a_record_read_under_the_wrong_id_is_refused() {
        let bytes = encode_promise("o:a", &promise(PromiseState::Pending, false), None);
        assert!(decode_promise("o:b", &bytes).is_err());
        // Another origin fails the codec's own origin check.
        assert!(decode_promise("x:a", &bytes).is_err());
    }

    #[test]
    fn an_origin_assembles_from_its_records_with_its_deadline() {
        let a = encode_promise("o:a", &promise(PromiseState::Pending, true), None);
        let b = encode_promise(
            "o:b",
            &promise(PromiseState::Pending, true),
            Some(&task(TaskState::Acquired)),
        );
        let doc = assemble([("o:a".to_string(), &a[..]), ("o:b".to_string(), &b[..])]).unwrap();
        assert_eq!(doc.promises.len(), 2);
        assert_eq!(doc.tasks.len(), 1);
        // The lease at 61_000 is earlier than either promise's 900_000.
        assert_eq!(doc.timer_at, Some(61_000));
    }

    #[test]
    fn the_diff_writes_only_what_changed() {
        let mut before = OriginDoc::default();
        before
            .promises
            .insert("o:a".into(), promise(PromiseState::Pending, false));
        before
            .promises
            .insert("o:b".into(), promise(PromiseState::Pending, true));
        before.tasks.insert("o:b".into(), task(TaskState::Acquired));
        before
            .promises
            .insert("o:gone".into(), promise(PromiseState::Resolved, false));

        let mut after = before.clone();
        // A task-only change still rewrites its promise's record.
        after.tasks.get_mut("o:b").unwrap().version = 4;
        after.promises.remove("o:gone");
        after
            .promises
            .insert("o:new".into(), promise(PromiseState::Pending, false));

        let out = diff(&before, &after);
        let ids: Vec<(&str, bool)> = out
            .iter()
            .map(|c| (c.id.as_str(), c.log.is_some()))
            .collect();
        for c in &out {
            assert_eq!(c.log.is_some(), c.local.is_some());
        }
        assert_eq!(ids, vec![("o:b", true), ("o:gone", false), ("o:new", true)]);
        assert!(diff(&after, &after).is_empty());
    }

    #[test]
    fn a_schedule_round_trips_and_a_newer_one_is_refused() {
        let doc = ScheduleDoc {
            v: SCHEDULE_FORMAT_VERSION,
            cron: "* * * * *".into(),
            promise_id: "s-{{.timestamp}}".into(),
            promise_timeout: 60_000,
            promise_param_headers: None,
            promise_param_data: None,
            promise_tags: BTreeMap::new(),
            created_at: 1,
            next_run_at: 60_000,
            last_run_at: None,
        };
        assert_eq!(decode_schedule(&encode_schedule(&doc)).unwrap(), doc);
        let mut newer = doc;
        newer.v = SCHEDULE_FORMAT_VERSION + 1;
        assert!(decode_schedule(&encode_schedule(&newer)).is_err());
    }
}
