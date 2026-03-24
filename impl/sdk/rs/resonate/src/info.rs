use std::collections::HashMap;

/// Read-only execution metadata for leaf functions.
/// Cannot spawn durable sub-tasks — no run/rpc methods.
#[derive(Debug, Clone)]
pub struct Info {
    id: String,
    parent_id: String,
    origin_id: String,
    branch_id: String,
    timeout_at: i64,
    func_name: String,
    tags: HashMap<String, String>,
}

impl Info {
    pub(crate) fn new(
        id: String,
        parent_id: String,
        origin_id: String,
        branch_id: String,
        timeout_at: i64,
        func_name: String,
        tags: HashMap<String, String>,
    ) -> Self {
        Self {
            id,
            parent_id,
            origin_id,
            branch_id,
            timeout_at,
            func_name,
            tags,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn parent_id(&self) -> &str {
        &self.parent_id
    }

    pub fn origin_id(&self) -> &str {
        &self.origin_id
    }

    pub fn branch_id(&self) -> &str {
        &self.branch_id
    }

    pub fn timeout_at(&self) -> i64 {
        self.timeout_at
    }

    pub fn func_name(&self) -> &str {
        &self.func_name
    }

    pub fn tags(&self) -> &HashMap<String, String> {
        &self.tags
    }
}
