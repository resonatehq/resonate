//! What a sandbox may ask of the server: its own task, and nothing else.
//!
//! The guest is untrusted code, and the plugin is the only thing between it
//! and a server it holds the credentials for. So every request is checked
//! against the task this sandbox was started for before it is forwarded —
//! an allow-list, by request kind:
//!
//! | kind                          | allowed when                                       |
//! |-------------------------------|----------------------------------------------------|
//! | `task.acquire`                | the claimed task, at the claimed version           |
//! | `task.heartbeat`              | every task listed is the claimed task              |
//! | `task.get`, `task.release`, `task.fulfill`, `task.suspend`, `task.fence` | the claimed task |
//! | `promise.get`                 | in the task's origin, or a root this sandbox created |
//! | `promise.create`              | in the task's origin, or a new root (as `task.fence` allows) |
//! | `promise.settle`              | in the task's origin, but not the task's own promise |
//! | `promise.register_callback`   | the awaiter is the claimed task                    |
//!
//! Everything else is refused: creating tasks, schedules, searching, listeners
//! (which would make the server deliver to an address of the guest's
//! choosing), the operator's `task.halt`/`task.continue`, and `debug.*`.
//!
//! This is the scope check, not validation. A request it lets through is
//! still validated by the server exactly as a remote worker's would be — the
//! fence's origin rule, the suspend's awaiter rule — and refused there if it
//! is malformed. What is decided here is only *whose* it is.

use std::collections::HashSet;

use serde_json::Value;

/// The task a sandbox was started for.
#[derive(Debug, Clone)]
pub struct Claim {
    pub id: String,
    pub version: i64,
    /// The lease the guest acquired, in ms, which a heartbeat renews.
    pub(crate) ttl: Option<u64>,
    /// Roots this sandbox created, which it may read back.
    created_roots: HashSet<String>,
}

impl Claim {
    pub fn new(id: impl Into<String>, version: i64) -> Self {
        Self {
            id: id.into(),
            version,
            ttl: None,
            created_roots: HashSet::new(),
        }
    }

    fn origin(&self) -> &str {
        origin(&self.id)
    }

    fn in_origin(&self, id: &str) -> bool {
        origin(id) == self.origin()
    }

    /// Note a root the server created at this sandbox's request.
    pub fn created(&mut self, kind: &str, data: &Value) {
        let id = match kind {
            "promise.create" => str_at(data, &["id"]),
            "task.fence" if str_at(data, &["action", "kind"]) == Some("promise.create") => {
                str_at(data, &["action", "data", "id"])
            }
            _ => None,
        };
        if let Some(id) = id {
            if !self.in_origin(id) {
                self.created_roots.insert(id.to_string());
            }
        }
    }

    /// `Ok` if a request of `kind` with `data` is this sandbox's to make.
    pub fn check(&self, kind: &str, data: &Value) -> Result<(), String> {
        let own_task = |field: &str| -> Result<(), String> {
            match str_at(data, &[field]) {
                Some(id) if id == self.id => Ok(()),
                Some(id) => Err(format!(
                    "{kind} on task {id}; this sandbox holds {}",
                    self.id
                )),
                None => Err(format!("{kind} without a task id")),
            }
        };
        match kind {
            "task.acquire" => {
                own_task("id")?;
                match data.get("version").and_then(Value::as_i64) {
                    Some(v) if v == self.version => Ok(()),
                    v => Err(format!(
                        "task.acquire at version {v:?}; this sandbox was started for {}",
                        self.version
                    )),
                }
            }
            "task.heartbeat" => {
                let tasks = data
                    .get("tasks")
                    .and_then(Value::as_array)
                    .ok_or("task.heartbeat without tasks")?;
                for t in tasks {
                    match str_at(t, &["id"]) {
                        Some(id) if id == self.id => {}
                        other => {
                            return Err(format!(
                                "task.heartbeat for task {other:?}; this sandbox holds {}",
                                self.id
                            ))
                        }
                    }
                }
                Ok(())
            }
            "task.get" | "task.release" | "task.fulfill" | "task.suspend" | "task.fence" => {
                own_task("id")
            }
            "promise.get" => {
                let id = str_at(data, &["id"]).ok_or("promise.get without an id")?;
                if self.in_origin(id) || self.created_roots.contains(id) {
                    Ok(())
                } else {
                    Err(format!(
                        "promise.get on {id}, outside origin {}",
                        self.origin()
                    ))
                }
            }
            "promise.create" => {
                let id = str_at(data, &["id"]).ok_or("promise.create without an id")?;
                // A root — an id that is its own origin — is how a detached
                // computation starts; `task.fence` allows it, so this does.
                if self.in_origin(id) || origin(id) == id {
                    Ok(())
                } else {
                    Err(format!("promise.create of {id} extends another origin"))
                }
            }
            "promise.settle" => {
                let id = str_at(data, &["id"]).ok_or("promise.settle without an id")?;
                if id == self.id {
                    Err("the task's own promise is settled by task.fulfill".into())
                } else if self.in_origin(id) {
                    Ok(())
                } else {
                    Err(format!(
                        "promise.settle of {id}, outside origin {}",
                        self.origin()
                    ))
                }
            }
            "promise.register_callback" => match str_at(data, &["awaiter"]) {
                Some(a) if a == self.id => Ok(()),
                a => Err(format!(
                    "promise.register_callback for awaiter {a:?}; this sandbox holds {}",
                    self.id
                )),
            },
            other => Err(format!("{other} is not available from a sandbox")),
        }
    }
}

/// Everything before the first ':' — the same rule the server applies.
fn origin(id: &str) -> &str {
    id.split_once(':').map(|(o, _)| o).unwrap_or(id)
}

fn str_at<'a>(v: &'a Value, path: &[&str]) -> Option<&'a str> {
    let mut v = v;
    for p in path {
        v = v.get(p)?;
    }
    v.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn claim() -> Claim {
        Claim::new("root:a.1", 3)
    }

    #[test]
    fn the_claimed_task_is_in_scope() {
        let c = claim();
        assert!(c
            .check(
                "task.acquire",
                &json!({"id": "root:a.1", "version": 3, "pid": "p", "ttl": 1})
            )
            .is_ok());
        for kind in [
            "task.get",
            "task.release",
            "task.fulfill",
            "task.suspend",
            "task.fence",
        ] {
            assert!(c.check(kind, &json!({"id": "root:a.1"})).is_ok(), "{kind}");
        }
        assert!(c
            .check(
                "task.heartbeat",
                &json!({"pid": "p", "tasks": [{"id": "root:a.1", "version": 3}]})
            )
            .is_ok());
    }

    #[test]
    fn another_task_is_not() {
        let c = claim();
        assert!(c
            .check("task.acquire", &json!({"id": "root:a.1", "version": 4}))
            .is_err());
        assert!(c
            .check("task.acquire", &json!({"id": "root:a.2", "version": 3}))
            .is_err());
        for kind in [
            "task.get",
            "task.release",
            "task.fulfill",
            "task.suspend",
            "task.fence",
        ] {
            assert!(c.check(kind, &json!({"id": "root:a.2"})).is_err(), "{kind}");
            assert!(c.check(kind, &json!({})).is_err(), "{kind}");
        }
        assert!(c
            .check(
                "task.heartbeat",
                &json!({"tasks": [{"id": "root:a.1"}, {"id": "root:b"}]})
            )
            .is_err());
    }

    #[test]
    fn promises_stay_in_the_origin() {
        let c = claim();
        assert!(c.check("promise.get", &json!({"id": "root"})).is_ok());
        assert!(c.check("promise.get", &json!({"id": "root:a.2"})).is_ok());
        assert!(c.check("promise.get", &json!({"id": "other:x"})).is_err());
        assert!(c
            .check("promise.create", &json!({"id": "root:a.1.1"}))
            .is_ok());
        assert!(c
            .check("promise.create", &json!({"id": "other:x"}))
            .is_err());
        assert!(c
            .check("promise.settle", &json!({"id": "root:a.1.1"}))
            .is_ok());
        assert!(c
            .check("promise.settle", &json!({"id": "root:a.1"}))
            .is_err());
        assert!(c.check("promise.settle", &json!({"id": "other"})).is_err());
    }

    #[test]
    fn a_root_it_created_can_be_read_back() {
        let mut c = claim();
        assert!(c.check("promise.get", &json!({"id": "detached"})).is_err());
        assert!(c
            .check("promise.create", &json!({"id": "detached"}))
            .is_ok());
        c.created("promise.create", &json!({"id": "detached"}));
        assert!(c.check("promise.get", &json!({"id": "detached"})).is_ok());

        c.created(
            "task.fence",
            &json!({"id": "root:a.1", "action": {"kind": "promise.create", "data": {"id": "fenced"}}}),
        );
        assert!(c.check("promise.get", &json!({"id": "fenced"})).is_ok());
    }

    #[test]
    fn callbacks_are_for_the_claimed_task() {
        let c = claim();
        assert!(c
            .check(
                "promise.register_callback",
                &json!({"awaited": "root:a.1.1", "awaiter": "root:a.1"})
            )
            .is_ok());
        assert!(c
            .check(
                "promise.register_callback",
                &json!({"awaited": "x", "awaiter": "root:b"})
            )
            .is_err());
    }

    #[test]
    fn everything_else_is_refused() {
        let c = claim();
        for kind in [
            "task.create",
            "task.search",
            "task.halt",
            "task.continue",
            "promise.search",
            "promise.register_listener",
            "schedule.create",
            "schedule.delete",
            "debug.tick",
            "ui.promises",
        ] {
            assert!(c.check(kind, &json!({"id": "root:a.1"})).is_err(), "{kind}");
        }
    }
}
