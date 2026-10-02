//! Pruning: a settled promise shadows its children.
//!
//! An id names its ancestors: `root:1.1.2` is a child of `root:1.1`, which is
//! a child of `root:1`, which is a child of the root `root`. The server holds
//! the lineage tags to the same tree (`resonate:parent` must prefix the id).
//! A promise is read by one thing: a replay of its parent's body. Once
//! `root:1.1` is settled its body never runs again, so nothing will ask for
//! `root:1.1.1` or `root:1.1.2` again: they are shadowed, and keeping them
//! only costs space.
//!
//! The rule: **a settled promise whose parent is settled is deleted, once
//! nothing under it is pending.**
//!
//! - The parent, not any ancestor. With `root:1` timed out while `root:1.1`
//!   still runs, a replay of `root:1.1` still reads `root:1.1.1`; deleting it
//!   would have the replay create it again and run its step twice.
//! - Only settled promises go. A child still pending when its parent settles
//!   — a body still running after its parent timed out — keeps working, and
//!   goes when it settles. Settling fulfils the promise's own task, so
//!   nothing a worker holds is ever deleted.
//! - A promise with something pending under it stays until that settles, so
//!   nothing is ever left without its parent. The promise a replay reads
//!   stays: a finished workflow collapses to its root.
//! - A detached child hangs off the origin, not its spawner, so it stays
//!   until the root settles.
//!
//! Every promise of an origin lives in that origin's document, so the rule is
//! decided on one document, and the deletions are tombstones in the same
//! commit, written after everything the settlement owed.
//!
//! Visible to clients: a pruned promise is gone, so `promise.get` on it is a
//! 404, and a request retried after its promise was pruned is answered as for
//! a promise that never existed — a `promise.create` makes it again, as
//! garbage that is pruned again when it settles. That is the price of the
//! space, and why this is off unless configured.

use resonate_core::types::{PromiseState, TaskState};
use resonate_server_blob::kernel::state::OriginDoc;

/// The proper ancestors of `id`, nearest first: `o:1.1.2` → `o:1.1`, `o:1`,
/// `o`. A root id (no `:`) has none.
pub fn ancestors(id: &str) -> Vec<&str> {
    let Some(colon) = id.find(':') else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut end = id.len();
    // Each '.' after the origin ends an ancestor, nearest last in the string.
    while let Some(dot) = id[colon + 1..end].rfind('.') {
        end = colon + 1 + dot;
        out.push(&id[..end]);
    }
    out.push(&id[..colon]);
    out
}

/// The prefix every descendant of `id` starts with: `o:` under the root `o`,
/// `o:1.` under `o:1`.
fn under(id: &str) -> String {
    let sep = if id.contains(':') { '.' } else { ':' };
    format!("{id}{sep}")
}

/// Delete from `doc` every settled promise whose parent is settled and under
/// which nothing is pending, with its task. Returns the ids deleted.
///
/// One pass is the whole fixpoint: a promise that goes has a settled parent
/// and nothing pending under it, so everything settled under it has a
/// settled parent and nothing pending under it too, and goes in the same
/// pass. Nothing is ever left whose parent went.
pub fn prune(doc: &mut OriginDoc) -> Vec<String> {
    let settled = |id: &str| {
        doc.promises
            .get(id)
            .is_some_and(|p| p.state != PromiseState::Pending)
    };
    let pending_under = |id: &str| {
        let prefix = under(id);
        doc.promises
            .range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .any(|(_, p)| p.state == PromiseState::Pending)
    };
    let gone: Vec<String> = doc
        .promises
        .keys()
        .filter(|id| settled(id))
        // Settling fulfils the task; one still live is a worker's, and stays.
        .filter(|id| {
            doc.tasks
                .get(id.as_str())
                .is_none_or(|t| t.state == TaskState::Fulfilled)
        })
        .filter(|id| ancestors(id).first().is_some_and(|parent| settled(parent)))
        .filter(|id| !pending_under(id))
        .cloned()
        .collect();
    for id in &gone {
        doc.promises.remove(id);
        doc.tasks.remove(id);
    }
    gone
}

#[cfg(test)]
mod tests {
    use super::*;
    use resonate_core::types::PromiseValue;
    use resonate_server_blob::kernel::state::{PromiseDoc, TaskDoc};

    #[test]
    fn ancestors_walk_the_id() {
        assert_eq!(ancestors("o:1.1.2"), vec!["o:1.1", "o:1", "o"]);
        assert_eq!(ancestors("o:1"), vec!["o"]);
        assert!(ancestors("o").is_empty());
        // Segments are whole: o:1.10 is not under o:1.1.
        assert_eq!(ancestors("o:1.10"), vec!["o:1", "o"]);
        // A detached child hangs off the origin.
        assert_eq!(ancestors("o:d3f2"), vec!["o"]);
        // A dotted origin is one segment.
        assert_eq!(ancestors("my.app:1.2"), vec!["my.app:1", "my.app"]);
    }

    fn promise(state: PromiseState) -> PromiseDoc {
        PromiseDoc {
            state,
            param: PromiseValue::default(),
            value: PromiseValue::default(),
            tags: Default::default(),
            timeout_at: 900_000,
            created_at: 1_000,
            settled_at: (state != Pending).then_some(2_000),
            callbacks: Vec::new(),
            listeners: Vec::new(),
        }
    }

    fn doc(promises: &[(&str, PromiseState)]) -> OriginDoc {
        let mut d = OriginDoc::default();
        for (id, s) in promises {
            d.promises.insert(id.to_string(), promise(*s));
        }
        d
    }

    fn ids(d: &OriginDoc) -> Vec<&str> {
        d.promises.keys().map(String::as_str).collect()
    }

    use PromiseState::{Pending, Rejected, Resolved};

    #[test]
    fn a_settled_promise_takes_its_settled_descendants() {
        let mut d = doc(&[
            ("o", Pending),
            ("o:1", Pending),
            ("o:1.1", Resolved),
            ("o:1.1.1", Resolved),
            ("o:1.1.2", Rejected),
            ("o:1.1.2.1", Resolved),
            ("o:1.2", Pending),
            ("o:1.10", Resolved),
        ]);
        let mut gone = prune(&mut d);
        gone.sort();
        assert_eq!(gone, vec!["o:1.1.1", "o:1.1.2", "o:1.1.2.1"]);
        // The shadowing promise stays, and so does everything beside it.
        assert_eq!(ids(&d), vec!["o", "o:1", "o:1.1", "o:1.10", "o:1.2"]);
    }

    #[test]
    fn a_pending_descendant_stays_until_it_settles() {
        let mut d = doc(&[("o", Pending), ("o:1", Resolved), ("o:1.1", Pending)]);
        assert!(prune(&mut d).is_empty());
        d.promises.get_mut("o:1.1").unwrap().state = Resolved;
        assert_eq!(prune(&mut d), vec!["o:1.1"]);
        assert_eq!(ids(&d), vec!["o", "o:1"]);
    }

    #[test]
    fn a_running_body_keeps_its_children_whatever_settled_above_it() {
        // o:1 timed out while o:1.1 still runs; a replay of o:1.1 reads o:1.1.1.
        let mut d = doc(&[
            ("o", Pending),
            ("o:1", Rejected),
            ("o:1.1", Pending),
            ("o:1.1.1", Resolved),
        ]);
        assert!(prune(&mut d).is_empty());
        // When o:1.1 settles, its child and then it go, in one pass.
        d.promises.get_mut("o:1.1").unwrap().state = Resolved;
        let mut gone = prune(&mut d);
        gone.sort();
        assert_eq!(gone, vec!["o:1.1", "o:1.1.1"]);
        assert_eq!(ids(&d), vec!["o", "o:1"]);
    }

    #[test]
    fn nothing_is_left_without_its_parent() {
        // o:1 is settled under a settled root, but o:1.1 is still pending.
        let mut d = doc(&[("o", Resolved), ("o:1", Resolved), ("o:1.1", Pending)]);
        assert!(prune(&mut d).is_empty(), "o:1 waits for what is under it");
        d.promises.get_mut("o:1.1").unwrap().state = Resolved;
        let mut gone = prune(&mut d);
        gone.sort();
        assert_eq!(gone, vec!["o:1", "o:1.1"]);
        assert_eq!(ids(&d), vec!["o"]);
    }

    #[test]
    fn a_finished_workflow_collapses_to_its_root() {
        let mut d = doc(&[
            ("o", Resolved),
            ("o:1", Resolved),
            ("o:1.1", Resolved),
            ("o:2", Resolved),
            ("o:d9", Resolved),
        ]);
        prune(&mut d);
        assert_eq!(ids(&d), vec!["o"]);
    }

    #[test]
    fn a_promise_created_without_its_parent_stays() {
        // o:1 was never created: nothing settled shadows o:1.1.
        let mut d = doc(&[("o:1.1", Resolved), ("o:1.1.1", Resolved)]);
        assert_eq!(prune(&mut d), vec!["o:1.1.1"]);
        assert_eq!(ids(&d), vec!["o:1.1"]);
    }

    #[test]
    fn its_task_goes_with_it_and_a_live_task_keeps_it() {
        let mut d = doc(&[("o", Resolved), ("o:1", Resolved), ("o:2", Resolved)]);
        let task = |state| TaskDoc {
            state,
            version: 1,
            pid: None,
            ttl: None,
            resumes: Default::default(),
            retry_at: None,
            lease_at: None,
        };
        d.tasks.insert("o:1".into(), task(TaskState::Fulfilled));
        d.tasks.insert("o:2".into(), task(TaskState::Acquired));
        assert_eq!(prune(&mut d), vec!["o:1"]);
        assert!(!d.tasks.contains_key("o:1"));
        assert!(d.promises.contains_key("o:2") && d.tasks.contains_key("o:2"));
    }
}
