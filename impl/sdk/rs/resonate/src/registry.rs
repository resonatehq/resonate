use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};

use crate::durable::{Durable, ExecutionEnv};
use crate::error::{Error, Result};
use crate::types::DurableKind;

/// Type-erased factory function for executing a registered durable function.
/// Wrapped in Arc so it can be cloned out of the registry while holding a read lock briefly.
pub type Factory = Arc<
    dyn for<'a> Fn(
            ExecutionEnv<'a>,
            serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = Result<serde_json::Value>> + Send + 'a>>
        + Send
        + Sync,
>;

/// An entry in the function registry.
pub struct RegistryEntry {
    pub name: String,
    pub kind: DurableKind,
    pub factory: Factory,
}

/// Maps function names to their implementations.
pub struct Registry {
    by_name: HashMap<String, RegistryEntry>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            by_name: HashMap::new(),
        }
    }

    /// Register a durable function.
    ///
    /// Returns an error if `name` is empty or already registered.
    pub fn add(&mut self, name: &str, kind: DurableKind, factory: Factory) -> Result<()> {
        if name.is_empty() {
            return Err(Error::Application {
                message: "name is required".to_string(),
            });
        }
        if self.by_name.contains_key(name) {
            return Err(Error::AlreadyRegistered(name.to_string()));
        }
        self.by_name.insert(
            name.to_string(),
            RegistryEntry {
                name: name.to_string(),
                kind,
                factory,
            },
        );
        Ok(())
    }

    /// Register a durable function from a `Durable` impl.
    /// The name and kind are derived from `D::NAME` and `D::KIND`.
    pub fn register<D, Args, T>(&mut self, func: D) -> Result<()>
    where
        D: Durable<Args, T> + Copy + Send + Sync + 'static,
        Args: DeserializeOwned + Send + 'static,
        T: Serialize + Send + 'static,
    {
        let factory: Factory = Arc::new(move |env, args_json| {
            Box::pin(async move {
                let args: Args = serde_json::from_value(args_json)?;
                let result = func.execute(env, args).await;
                match result {
                    Ok(val) => serde_json::to_value(val).map_err(Into::into),
                    Err(e) => Err(e),
                }
            })
        });
        self.add(D::NAME, D::KIND, factory)
    }

    /// Look up a registered function by name.
    pub fn get(&self, name: &str) -> Option<&RegistryEntry> {
        self.by_name.get(name)
    }

    /// Check if a function is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }

    /// Get all registered function names.
    pub fn names(&self) -> Vec<&str> {
        self.by_name.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_factory() -> Factory {
        Arc::new(|_env, _args| Box::pin(async { Ok(serde_json::Value::Null) }))
    }

    #[test]
    fn register_a_function_by_name() {
        let mut registry = Registry::new();
        registry
            .add("foo", DurableKind::Function, dummy_factory())
            .unwrap();
        let entry = registry.get("foo");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().name, "foo");
    }

    #[test]
    fn register_with_custom_name() {
        let mut registry = Registry::new();
        registry
            .add("bar*", DurableKind::Function, dummy_factory())
            .unwrap();
        let entry = registry.get("bar*");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().name, "bar*");
    }

    #[test]
    fn reject_duplicate_name_registration() {
        let mut registry = Registry::new();
        registry
            .add("baz", DurableKind::Function, dummy_factory())
            .unwrap();
        let err = registry.add("baz", DurableKind::Function, dummy_factory());
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("already registered"));
    }

    #[test]
    fn returns_none_for_unregistered_function() {
        let registry = Registry::new();
        assert!(registry.get("doesNotExist").is_none());
    }

    #[test]
    fn reject_functions_without_a_name() {
        let mut registry = Registry::new();
        let err = registry.add("", DurableKind::Function, dummy_factory());
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("name is required"));
    }
}
