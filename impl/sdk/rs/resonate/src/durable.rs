use std::future::Future;

use crate::context::Context;
use crate::error::Result;
use crate::info::Info;
use crate::types::DurableKind;

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
    /// - `ctx` is `Some` for workflows (DurableKind::Workflow).
    /// - `info` is `Some` for leaf functions with metadata (DurableKind::Function with Info).
    /// - Both are `None` for pure leaf functions.
    fn execute(
        &self,
        ctx: Option<&Context>,
        info: Option<&Info>,
        args: Args,
    ) -> impl Future<Output = Result<T>> + Send;
}
