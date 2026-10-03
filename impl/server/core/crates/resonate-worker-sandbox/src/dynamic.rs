//! Backends, type-erased, so one worker can hold several.
//!
//! [`Backend`] is the trait a backend implements: associated types and
//! `impl Future`, which is the right shape to write one in and cannot be put
//! behind a `dyn`. The worker routes each address to one of several backends,
//! so it holds them as [`AnyBackend`] — the same three calls, boxed — and
//! every `Backend` is one by the blanket impl below.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;

use resonate_sandbox::{Backend, Command, Process, Stderr, Stdin, Stdout};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A sandbox, whichever backend made it. Only that backend can open it.
pub type AnyHandle = Box<dyn Any + Send + Sync>;

pub trait AnyBackend: Send + Sync {
    fn create<'a>(&'a self, image: &'a str) -> BoxFuture<'a, Result<AnyHandle, String>>;
    fn exec<'a>(
        &'a self,
        handle: &'a AnyHandle,
        command: Command,
    ) -> BoxFuture<'a, Result<Box<dyn AnyProcess>, String>>;
    fn destroy(&self, handle: AnyHandle) -> BoxFuture<'_, Result<(), String>>;
}

pub trait AnyProcess: Send {
    fn stdin(&mut self) -> Option<Stdin>;
    fn stdout(&mut self) -> Option<Stdout>;
    fn stderr(&mut self) -> Option<Stderr>;
    fn wait(&mut self) -> BoxFuture<'_, std::io::Result<i32>>;
}

impl<B> AnyBackend for B
where
    B: Backend,
    B::Handle: 'static,
{
    fn create<'a>(&'a self, image: &'a str) -> BoxFuture<'a, Result<AnyHandle, String>> {
        Box::pin(async move {
            Backend::create(self, image)
                .await
                .map(|h| Box::new(h) as AnyHandle)
                .map_err(|e| e.to_string())
        })
    }

    fn exec<'a>(
        &'a self,
        handle: &'a AnyHandle,
        command: Command,
    ) -> BoxFuture<'a, Result<Box<dyn AnyProcess>, String>> {
        Box::pin(async move {
            let handle = handle
                .downcast_ref::<B::Handle>()
                .ok_or("a sandbox handed to a backend that did not make it")?;
            Backend::exec(self, handle, command)
                .await
                .map(|p| Box::new(p) as Box<dyn AnyProcess>)
                .map_err(|e| e.to_string())
        })
    }

    fn destroy(&self, handle: AnyHandle) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let handle = handle
                .downcast::<B::Handle>()
                .map_err(|_| "a sandbox handed to a backend that did not make it")?;
            Backend::destroy(self, *handle)
                .await
                .map_err(|e| e.to_string())
        })
    }
}

impl<P: Process> AnyProcess for P {
    fn stdin(&mut self) -> Option<Stdin> {
        Process::stdin(self)
    }

    fn stdout(&mut self) -> Option<Stdout> {
        Process::stdout(self)
    }

    fn stderr(&mut self) -> Option<Stderr> {
        Process::stderr(self)
    }

    fn wait(&mut self) -> BoxFuture<'_, std::io::Result<i32>> {
        Box::pin(Process::wait(self))
    }
}
