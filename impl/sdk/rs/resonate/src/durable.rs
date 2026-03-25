use std::future::Future;

use crate::context::Context;
use crate::error::Result;
use crate::info::Info;
use crate::types::DurableKind;

/// The execution environment passed to a durable function.
///
/// - `Function(Info)` — leaf functions receive read-only metadata.
/// - `Workflow(Context)` — workflow functions receive a full context for sub-tasks.
pub enum ExecutionEnv<'a> {
    Function(&'a Info),
    Workflow(&'a Context),
}

impl<'a> ExecutionEnv<'a> {
    /// Extract the `Context` reference, panicking if this is a `Function` env.
    pub fn into_context(self) -> &'a Context {
        match self {
            ExecutionEnv::Workflow(ctx) => ctx,
            ExecutionEnv::Function(_) => {
                panic!("expected Workflow ExecutionEnv, got Function")
            }
        }
    }

    /// Extract the `Info` reference, panicking if this is a `Workflow` env.
    pub fn into_info(self) -> &'a Info {
        match self {
            ExecutionEnv::Function(info) => info,
            ExecutionEnv::Workflow(_) => {
                panic!("expected Function ExecutionEnv, got Workflow")
            }
        }
    }
}

/// Trait implemented by all `#[resonate::function]`-annotated functions.
/// Provides name/kind metadata and a uniform execution interface.
///
/// Type parameters:
/// - `Args`: The function's input arguments (must be serializable).
/// - `T`: The function's return type (must be serializable).
pub trait Durable<Args, T>: Send + Sync + 'static {
    /// The registered name of this function (used for durable promise lookup).
    const NAME: &'static str;

    /// Whether this is a leaf (Function) or a workflow (Workflow).
    const KIND: DurableKind;

    /// Execute the function.
    ///
    /// The `env` parameter provides either a `Context` (for workflows) or
    /// an `Info` (for leaf functions). Pure leaf functions may ignore it.
    fn execute(
        &self,
        env: ExecutionEnv<'_>,
        args: Args,
    ) -> impl Future<Output = Result<T>> + Send;
}
