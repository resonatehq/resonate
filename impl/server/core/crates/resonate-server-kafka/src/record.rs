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
//!   document to another needs, and nothing for what did not change.
//!
//! # Dependencies
//!
//! The blob crate's kernel types and codec, and its schedule document.
//!
//! # Dependants
//!
//! The partition shell encodes with [`diff`] before every commit and decodes
//! with [`assemble`] on every load; restore and search decode stored records.

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

/// The records that take an origin from `before` to `after`: a value for every
/// promise whose promise or task changed, a tombstone for every promise that
/// is gone. Unchanged promises produce nothing.
pub fn diff(before: &OriginDoc, after: &OriginDoc) -> Vec<(String, Option<Vec<u8>>)> {
    let ids: BTreeSet<&String> = before.promises.keys().chain(after.promises.keys()).collect();
    let mut out = Vec::new();
    for id in ids {
        let old = (before.promises.get(id), before.tasks.get(id));
        let new = (after.promises.get(id), after.tasks.get(id));
        if old == new {
            continue;
        }
        match new.0 {
            Some(promise) => out.push((id.clone(), Some(encode_promise(id, promise, new.1)))),
            None => out.push((id.clone(), None)),
        }
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
        before
            .tasks
            .insert("o:b".into(), task(TaskState::Acquired));
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
            .map(|(id, v)| (id.as_str(), v.is_some()))
            .collect();
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
