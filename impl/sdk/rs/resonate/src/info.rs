use std::collections::HashMap;
use std::sync::Arc;

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
    deps: Arc<crate::DependencyMap>,
}

impl Info {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: String,
        parent_id: String,
        origin_id: String,
        branch_id: String,
        timeout_at: i64,
        func_name: String,
        tags: HashMap<String, String>,
        deps: Arc<crate::DependencyMap>,
    ) -> Self {
        Self {
            id,
            parent_id,
            origin_id,
            branch_id,
            timeout_at,
            func_name,
            tags,
            deps,
        }
    }

    /// Retrieve a dependency by type. Panics if not found.
    pub fn get_dependency<T: Send + Sync + 'static>(&self) -> Arc<T> {
        self.deps.get::<T>()
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
