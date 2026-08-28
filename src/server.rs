use std::sync::Arc;

use crate::config::Config;
use async_trait::async_trait;
use resonate_core::router::ResonateRouter;
use resonate_core::types::{
    ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, Message, MessageHead, UnblockMsg, UnblockMsgData,
    UnblockMsgHead,
};
use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_core::util;
use resonate_core::{ResonateServer, Unavailable};
use resonate_server_dbms::engine_port::{
    Input, Outgoing, Output, ResonateEngine, Scheduled, Timeout,
};

use crate::deadlines::DeadlineTimer;
use crate::metrics;

/// The running server — owns configuration, the engine, the router and the timer.
pub struct Server {
    pub config: Config,
    /// Durable state and every transition over it. The server validates,
    /// hands over, and shapes what comes back.
    pub engine: Arc<dyn ResonateEngine>,
    /// Where a transition's messages go.
    ///
    /// A server without one could not deliver anything it produced, so it is
    /// not optional and not late-bound: `deliver` has a router or the server
    /// does not exist. The ring — server holds router, router holds workers,
    /// worker holds server — is closed by `Arc::new_cyclic` at startup, which
    /// puts the one weak link on the workers' handle, where it belongs: a
    /// worker outliving its server is a real condition at shutdown, whereas a
    /// server without a router was only ever an artifact of the wiring order.
    router: Arc<dyn ResonateRouter>,
    /// The near future, in memory.
    ///
    /// Every deadline a transition arms is merged here, and the timer asks the
    /// engine for the one it names the moment it comes due. A cache, not a
    /// record: what it holds is a bounded prefix of what one process has heard
    /// about, and the sweep in `processing_timeouts` is what covers the rest.
    timer: DeadlineTimer,
}

impl Server {
    pub fn new(
        config: Config,
        engine: Arc<dyn ResonateEngine>,
        router: Arc<dyn ResonateRouter>,
        timer: DeadlineTimer,
    ) -> Self {
        Self {
            engine,
            config,
            router,
            timer,
        }
    }

    /// Start the timer and wait for it to be seeded.
    ///
    /// Separate from construction because seeding reads the database, and
    /// because the timer's own callbacks point back here: nothing can run until
    /// the server is behind an `Arc`.
    pub async fn start_timer(&self) {
        self.timer.init().await;
    }

    /// Stop the timer task.
    pub async fn stop_timer(&self) {
        self.timer.stop().await;
    }

    /// Hand the timer the deadlines a transition just armed.
    ///
    /// Cheap and lossy on purpose: a send onto a queue, no waiting, and a
    /// dropped batch costs latency rather than correctness — the durable row
    /// committed with the transition, and the sweep still finds it. The wheel
    /// decides what is worth keeping, so everything is offered to it.
    pub fn arm(&self, timeouts: Vec<Scheduled>) {
        // A paused engine is on a clock `debug.tick` drives, and the wheel is
        // on the wall clock. Feeding one to the other would fill it with
        // deadlines that are due at a fictional instant, and `fire` would
        // decline every one of them anyway. Backfill re-reads the world when
        // the engine resumes.
        if timeouts.is_empty() || self.engine.is_paused() {
            return;
        }
        self.timer.merge(
            timeouts
                .into_iter()
                .map(crate::deadlines::scheduled_to_entry)
                .collect(),
        );
    }

    /// Fire deadlines the timer says have come due.
    ///
    /// One `Internal` per deadline, which is the narrow form: the engine acts
    /// on the row that timeout names and nothing else. Firing is a hint, so
    /// each of these may find the deadline has moved or the row has settled and
    /// do nothing — that is `Internal` being idempotent, and it is what lets
    /// this run alongside a sweep that will fire the same deadlines.
    ///
    /// What comes back is treated exactly like a request's output: messages go
    /// to the router, and a deadline a firing armed goes straight back into the
    /// timer, which is how a redispatched task keeps its retry deadline live
    /// without a round trip through the sweep.
    pub async fn fire(&self, timeouts: Vec<Timeout>) {
        if self.engine.is_paused() {
            return;
        }
        let now = util::system_time_ms();
        for timeout in timeouts {
            let out = self.engine.process(Input::Internal(timeout), now).await;
            self.deliver(out.messages).await;
            self.arm(out.timeouts);
        }
    }

    /// Deliver what a transition emitted.
    ///
    /// This is what the message pump used to do on a 100 ms poll, over rows a
    /// transition had left in an outbox. There is no queue between the two any
    /// more: a message goes out as soon as the transaction that produced it has
    /// committed.
    ///
    /// Delivery is best-effort, as it was: a failed route is logged and the
    /// attempt is lost. An execute message comes back — the task stays pending
    /// and its retry timeout re-emits it — and an unblock message does not,
    /// which is the behaviour the outbox had too, since the pump deleted before
    /// it delivered.
    pub async fn deliver(&self, messages: Vec<Outgoing>) {
        if messages.is_empty() {
            return;
        }
        let server_url = self.config.server.url.clone().unwrap_or_default();
        for msg in messages {
            let (address, payload) = match msg {
                Outgoing::Execute {
                    address,
                    task_id,
                    version,
                } => {
                    metrics::MESSAGES_TOTAL
                        .with_label_values(&["execute"])
                        .inc();
                    tracing::info!(kind = "execute", task_id = %task_id, version, address = %address, "Dispatching execute message");
                    (
                        address,
                        Message::Execute(ExecuteMsg {
                            kind: "execute".to_string(),
                            head: MessageHead {
                                server_url: server_url.clone(),
                            },
                            data: ExecuteMsgData {
                                task: ExecuteMsgTask {
                                    id: task_id,
                                    version,
                                },
                            },
                        }),
                    )
                }
                Outgoing::Unblock { address, promise } => {
                    metrics::MESSAGES_TOTAL
                        .with_label_values(&["unblock"])
                        .inc();
                    tracing::info!(kind = "unblock", promise_id = %promise.id, promise_state = %promise.state, address = %address, "Dispatching unblock message");
                    (
                        address,
                        Message::Unblock(UnblockMsg {
                            kind: "unblock".to_string(),
                            head: UnblockMsgHead {},
                            data: UnblockMsgData { promise },
                        }),
                    )
                }
            };
            if let Err(e) = self.router.route(&address, &payload).await {
                tracing::warn!(address = %address, error = %e, "Message not delivered");
            }
        }
    }
}

#[async_trait]
impl ResonateServer for Server {
    /// Ready when storage answers. That is the only thing that can be down and
    /// still leave this process running.
    async fn ready(&self) -> bool {
        match self.engine.ping().await {
            Ok(()) => true,
            Err(e) => {
                tracing::error!(error = %e, "Readiness check failed: storage database unavailable");
                false
            }
        }
    }

    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        // Debug-time overrides are gated by config, so a caller cannot move the
        // server's clock. The gate lives here rather than at the HTTP edge so
        // that every caller of the port is subject to it.
        let debug_time = if self.config.debug {
            req.head.debug_time
        } else {
            None
        };
        let Output {
            response,
            messages,
            timeouts,
        } = self
            .engine
            .process(Input::External(req), util::resolve_time(debug_time))
            .await;
        // Deliver after the transition has committed, never before: the engine
        // returns only what its transaction actually wrote.
        self.deliver(messages).await;
        self.arm(timeouts);
        Ok(response.expect("invariant: External input always yields a response"))
    }
}
