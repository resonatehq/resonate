use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crate::engine::{Engine, Input, Outgoing, Output, Scheduled, Timeout};
use async_trait::async_trait;
use resonate_core::router::ResonateRouter;
use resonate_core::types::{
    ExecuteMsg, ExecuteMsgData, ExecuteMsgTask, Message, MessageHead, UnblockMsg, UnblockMsgData,
    UnblockMsgHead,
};
use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_core::util;
use resonate_core::{ResonateServer, Unavailable};

use crate::deadlines::DeadlineTimer;
use crate::metrics;

/// This server: its engine, a router to deliver through, a timer for the near
/// future, and a sweep behind both.
///
/// Its own copy, not the SQL family's. Depending on `resonate-sql` for it would
/// have made a ScyllaDB server depend on a crate called sql, which is the shape
/// that went wrong before — and independence is what lets the eager mechanics
/// here diverge from the lazy ones there without negotiating.
///
/// Takes arguments, not configuration. Reading `[servers.<id>]` is the
/// plugin's, at its edge; by the time anything gets here it is values.
pub struct Server {
    /// The externally reachable URL, stamped into every message so a worker
    /// knows where to call back.
    server_url: String,
    /// How often the sweep runs, in milliseconds.
    sweep_interval: u64,
    /// Durable state and every transition over it. The server validates,
    /// hands over, and shapes what comes back.
    ///
    /// Absent until `init` has connected. Opening a database is I/O that can
    /// fail, which belongs in `init` for the same reason it does in every other
    /// port — so `configure` stays sync, cheap and side-effect-free.
    engine: OnceLock<Arc<dyn Engine>>,
    /// How to get one. Taken by `init`, and gone after.
    connect: Mutex<Option<Connect>>,
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
    /// The process-wide debug flag, which arrives at `init` because that is
    /// where every port receives it.
    ///
    /// One flag, set once at startup, rather than a mode a request can enter.
    /// It says the clock belongs to the caller: `head.debug_time` is honoured,
    /// the `debug.*` operations are answered, and nothing in this process runs
    /// on wall time — no sweep, no timer. A server that could be put into that
    /// state by a request had to be asked, at every step, whether it was in it;
    /// a server that is told once at startup does not.
    debug: AtomicBool,
    /// The near future, in memory.
    ///
    /// Every deadline a transition arms is merged here, and the timer asks the
    /// engine for the one it names the moment it comes due. A cache, not a
    /// record: what it holds is a bounded prefix of what one process has heard
    /// about, and the sweep is what covers the rest.
    timer: DeadlineTimer,
    /// The sweep, once `init` has started it, and the switch that ends it.
    sweep: Mutex<Option<tokio::task::JoinHandle<()>>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    /// A handle to itself, for the two background tasks that call back in.
    /// Weak, or the server would keep itself alive forever.
    this: std::sync::Weak<Server>,
}

/// Open the durable state. Run by `init`, given the process-wide debug flag —
/// which the engine needs too, since it is what gates the `debug.*` operations.
pub type Connect = Box<
    dyn FnOnce(
            bool,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Arc<dyn Engine>, Unavailable>> + Send>,
        > + Send,
>;

/// What a server needs that its engine does not carry. Plain values: the
/// plugin read the configuration, this is what came out.
#[derive(Debug, Clone)]
pub struct Options {
    /// Stamped into every emitted message.
    pub server_url: String,
    /// How many deadlines the in-memory timer holds.
    pub wheel_capacity: usize,
    /// How often it re-reads the durable deadlines, and the longest it sleeps.
    pub wheel_refresh: u64,
    /// The backstop scan interval.
    pub sweep_interval: u64,
}

impl Server {
    /// Build one. Nothing is connected, seeded or spawned — that is `init`.
    ///
    /// `Arc` rather than `Self`, because the timer's callbacks point back here
    /// and so the handle has to exist before the value does. That cycle is this
    /// crate's own business; the composition root sees a `ResonateServer`.
    pub fn new(connect: Connect, router: Arc<dyn ResonateRouter>, options: Options) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            timer: crate::deadlines::build(
                options.wheel_capacity,
                options.wheel_refresh,
                weak.clone(),
            ),
            server_url: options.server_url,
            sweep_interval: options.sweep_interval,
            engine: OnceLock::new(),
            connect: Mutex::new(Some(connect)),
            router,
            debug: AtomicBool::new(false),
            sweep: Mutex::new(None),
            shutdown: tokio::sync::watch::channel(false).0,
            this: weak.clone(),
        })
    }

    /// Whether the clock belongs to the caller. Set by `init`.
    pub fn debug(&self) -> bool {
        self.debug.load(Ordering::Relaxed)
    }

    /// The engine, once `init` has opened it.
    ///
    /// Nothing reaches a server before it is started — a gateway binds last —
    /// so this is unreachable in a running process. It is an error rather than
    /// a panic because a wiring mistake should be reported, not abort.
    pub fn engine(&self) -> Result<&Arc<dyn Engine>, Unavailable> {
        self.engine
            .get()
            .ok_or_else(|| Unavailable::new("server is not started"))
    }

    /// Hand the timer the deadlines a transition just armed.
    ///
    /// Cheap and lossy on purpose: a send onto a queue, no waiting, and a
    /// dropped batch costs latency rather than correctness — the durable row
    /// committed with the transition, and the sweep still finds it. The wheel
    /// decides what is worth keeping, so everything is offered to it.
    pub fn arm(&self, timeouts: Vec<Scheduled>) {
        // Under the debug flag there is no timer to arm — it was never
        // started, because the clock belongs to the caller.
        if timeouts.is_empty() || self.debug() {
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
        let now = util::system_time_ms();
        for timeout in timeouts {
            let Ok(engine) = self.engine() else {
                return;
            };
            let out = engine.process(Input::Internal(timeout), now).await;
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
        let server_url = self.server_url.clone();
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
    /// Seed the timer, start the sweep, and remember the clock.
    ///
    /// Seeding reads the database, which is why it is here and not in `new`: a
    /// deadline already due fires immediately rather than waiting for the first
    /// scan. `init` returns only once that read has landed.
    ///
    /// Under debug neither runs. The clock belongs to the caller, so the timer
    /// — which *is* the clock — must not start, and `debug.tick` moves time
    /// instead, sweeping as part of moving.
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        self.debug.store(debug, Ordering::Relaxed);
        // Registration is on first use, so without this a counter that has not
        // been incremented is absent from /metrics rather than zero — and those
        // mean different things to whoever is reading the dashboard.
        crate::metrics::declare();

        let connect = self
            .connect
            .lock()
            .expect("connect mutex")
            .take()
            .ok_or_else(|| Unavailable::new("server was already started"))?;
        let engine = connect(debug).await?;
        let _ = self.engine.set(engine);

        if debug {
            tracing::warn!(
                "Debug mode — no timer and no sweep. Time advances only through \
                 debug.tick, and debug.* operations are answered."
            );
            return Ok(());
        }

        self.timer.init().await;
        let handle = tokio::spawn(crate::sweep::run(
            self.this.clone(),
            self.sweep_interval,
            self.shutdown.subscribe(),
        ));
        *self.sweep.lock().expect("sweep mutex") = Some(handle);
        tracing::info!(
            sweep_interval_ms = self.sweep_interval,
            "Timer and sweep started"
        );
        Ok(())
    }

    /// Stop the timer first, then drain the sweep.
    ///
    /// The timer is the only thing that can still hand the engine work of its
    /// own, so stopping it means nothing new arrives while the sweep finishes
    /// whatever it is in the middle of.
    async fn stop(&self) -> Result<(), Unavailable> {
        self.timer.stop().await;
        let _ = self.shutdown.send(true);
        let handle = self.sweep.lock().expect("sweep mutex").take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        Ok(())
    }

    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        // Debug-time overrides are gated by config, so a caller cannot move the
        // server's clock. The gate lives here rather than at the HTTP edge so
        // that every caller of the port is subject to it.
        let debug_time = if self.debug() {
            req.head.debug_time
        } else {
            None
        };
        let Output {
            response,
            messages,
            timeouts,
        } = self
            .engine()?
            .process(Input::External(req), util::resolve_time(debug_time))
            .await;
        // Deliver after the transition has committed, never before: the engine
        // returns only what its transaction actually wrote.
        self.deliver(messages).await;
        self.arm(timeouts);
        Ok(response.expect("invariant: External input always yields a response"))
    }
}
