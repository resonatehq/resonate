//! This server, as a plugin.
//!
//! The crate below is a complete `ResonateServer` and knew nothing about how a
//! binary was assembled; this module is the only part that does. It reads
//! `[servers.server_blob]`, defers every connection to `init`, and drives the
//! timer loop from the port's own lifecycle — so the composition root treats it
//! exactly like the four SQL servers and needs no branch of its own.
//!
//! What it replaced was a 150-line `run_blob_server` in `main.rs`, reached by a
//! `storage.type == "blob"` test, which rebuilt the router, the workers, the
//! metrics endpoint and the gateway a second time because the object store is
//! not a SQL engine. None of that was about blobs.

use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use resonate_core::router::ResonateRouter;
use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_core::{ResonateServer, Unavailable};

use crate::applier::{ApplierCfg, KeySpace};
use crate::kernel::state::KernelCfg;
use crate::server::{Server, ServerCfg};
use crate::store::{ObjectStoreAdapter, Store};

/// This server, as a plugin. The one thing a binary names to store in a bucket.
pub static PLUGIN: resonate_plugin::ServerPlugin =
    resonate_plugin::ServerPlugin::new(env!("CARGO_PKG_NAME"), configure);

/// Read `[servers.server_blob]`, and build the server without touching the
/// bucket.
fn configure(
    settings: &resonate_plugin::Settings<'_>,
    deps: resonate_plugin::ServerDependencies,
) -> Result<Arc<dyn ResonateServer>, resonate_plugin::ConfigError> {
    let config: Config = settings.extract()?;
    if config.timer_shards == 0 {
        return Err(settings.reject("timer_shards", "must be at least 1 (got 0)"));
    }
    if config.cache_capacity == 0 {
        return Err(settings.reject("cache_capacity", "must be at least 1 (got 0)"));
    }
    Ok(Arc::new(BlobServer::new(config, deps.router)))
}

/// Everything under `[servers.server_blob]`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Bucket holding every object. When unset the backend runs against an
    /// in-process, in-memory store — nothing survives the process. That is a
    /// test and development mode, and startup says so loudly.
    #[serde(default)]
    pub bucket: Option<String>,

    /// Region. Defaults to whatever the environment or instance metadata says.
    #[serde(default)]
    pub region: Option<String>,

    /// Endpoint override, for an S3-compatible service.
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Allow a plain-HTTP endpoint. Only for a local test service.
    #[serde(default)]
    pub allow_http: bool,

    /// Prefix under which every key lives, so one bucket can hold several
    /// deployments.
    #[serde(default)]
    pub prefix: String,

    /// How many prefixes the timer keys are spread across.
    ///
    /// Timer keys carry their deadline, so they increase monotonically — the
    /// classic S3 hot-prefix anti-pattern. Sharding spreads the writes.
    #[serde(default = "default_timer_shards")]
    pub timer_shards: u32,

    /// Documents held in the read cache.
    #[serde(default = "default_cache_capacity")]
    pub cache_capacity: usize,

    /// How many times a batch is re-decided after losing a race before the
    /// caller is told there is no answer.
    #[serde(default = "default_max_cas_retries")]
    pub max_cas_retries: u32,

    /// How many branch siblings a task response may carry.
    #[serde(default = "default_preload_limit")]
    pub preload_limit: u32,

    /// Whether the search operations are answered. Each search reads every
    /// document in the store — O(origins) GETs — so they are off unless asked
    /// for.
    #[serde(default)]
    pub search_enabled: bool,

    /// The URL a worker is told to call back on, stamped into every execute
    /// message. The same key every server plugin carries.
    #[serde(default)]
    pub server_url: String,

    /// How long a task may be held before it is handed to someone else.
    #[serde(default = "default_retry_timeout")]
    pub retry_timeout: i64,
}

fn default_timer_shards() -> u32 {
    4
}
fn default_cache_capacity() -> usize {
    10_000
}
fn default_max_cas_retries() -> u32 {
    8
}
fn default_preload_limit() -> u32 {
    10
}
fn default_retry_timeout() -> i64 {
    60_000
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bucket: None,
            region: None,
            endpoint: None,
            allow_http: false,
            prefix: String::new(),
            timer_shards: default_timer_shards(),
            cache_capacity: default_cache_capacity(),
            max_cas_retries: default_max_cas_retries(),
            preload_limit: default_preload_limit(),
            search_enabled: false,
            server_url: String::new(),
            retry_timeout: default_retry_timeout(),
        }
    }
}

/// The plugin: config and a router now, a bucket and a timer loop after `init`.
///
/// The deferral is the same one every SQL server makes, for the same reason —
/// building the store reads credentials and stands up an HTTP client, which is
/// I/O, and `configure` is sync and side-effect free by contract. Doing it in
/// `init` is also what makes an unreachable bucket a startup failure rather
/// than a request answered wrongly later.
pub struct BlobServer {
    config: Config,
    router: Arc<dyn ResonateRouter>,
    /// The real server, once `init` has built it.
    inner: OnceLock<Arc<Server>>,
    /// Ends the timer loop. Held so `stop` can signal it.
    shutdown: tokio::sync::watch::Sender<bool>,
    /// The timer loop, so `stop` can wait for it to finish.
    timer: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl BlobServer {
    pub fn new(config: Config, router: Arc<dyn ResonateRouter>) -> Self {
        Self {
            config,
            router,
            inner: OnceLock::new(),
            shutdown: tokio::sync::watch::channel(false).0,
            timer: Mutex::new(None),
        }
    }

    /// The server behind the port, or a plain error saying it was never started.
    fn started(&self) -> Result<&Arc<Server>, Unavailable> {
        self.inner
            .get()
            .ok_or_else(|| Unavailable::new("the blob server has not been started"))
    }

    /// The bucket, or the in-memory stand-in when none is named.
    fn open_store(&self) -> Result<Arc<dyn Store>, Unavailable> {
        let Some(bucket) = &self.config.bucket else {
            tracing::warn!(
                "servers.server_blob.bucket is not set — using an in-process, \
                 in-memory object store. Nothing survives this process."
            );
            return Ok(Arc::new(ObjectStoreAdapter::in_memory()));
        };

        tracing::info!(
            bucket = %bucket,
            prefix = %self.config.prefix,
            timer_shards = self.config.timer_shards,
            "Using blob backend"
        );
        tracing::warn!(
            "The blob backend requires real conditional writes (If-Match / \
             If-None-Match). S3, R2, GCS and Azure qualify; MinIO, B2 and \
             Spaces do not, and lose writes silently."
        );

        let mut builder = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);
        if let Some(region) = &self.config.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &self.config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if self.config.allow_http {
            builder = builder.with_allow_http(true);
        }
        let s3 = builder
            .build()
            .map_err(|e| Unavailable::new(format!("cannot configure the S3 store: {e}")))?;
        Ok(Arc::new(ObjectStoreAdapter::new(s3)))
    }
}

#[async_trait]
impl ResonateServer for BlobServer {
    /// Open the store, build the server, and start the timer loop.
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        let store = self.open_store()?;
        let server = Server::build(
            store,
            Arc::clone(&self.router),
            ServerCfg {
                keys: KeySpace::new(self.config.prefix.clone(), self.config.timer_shards),
                applier: ApplierCfg {
                    max_cas_retries: self.config.max_cas_retries,
                    kernel: KernelCfg {
                        retry_timeout: self.config.retry_timeout,
                        preload_limit: self.config.preload_limit,
                        // Overridden at build from `server_url` below.
                        ..Default::default()
                    },
                    ..Default::default()
                },
                timerd: Default::default(),
                cache_capacity: self.config.cache_capacity,
                debug,
                search: self.config.search_enabled,
                server_url: self.config.server_url.clone(),
            },
        );

        // Under the debug flag the clock belongs to the caller, so the loop is
        // never spawned: time advances through `debug.tick` and nowhere else.
        if debug {
            tracing::warn!(
                "Debug mode — no timer loop, and messages are held for debug.snap. \
                 Time advances only through debug.tick, and debug.* operations \
                 are answered."
            );
        } else {
            let handle = Arc::clone(server.timerd()).spawn(self.shutdown.subscribe());
            *self.timer.lock().expect("blob timer mutex") = Some(handle);
        }

        let _ = self.inner.set(server);
        Ok(())
    }

    /// Stop the timer loop and wait for it.
    ///
    /// Safe when `init` never ran: there is no handle to take, and signalling a
    /// channel nobody is listening to is not an error.
    async fn stop(&self) -> Result<(), Unavailable> {
        let _ = self.shutdown.send(true);
        // Out of the guard before the await: a std MutexGuard is not Send.
        let handle = self.timer.lock().expect("blob timer mutex").take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        Ok(())
    }

    async fn process(&self, req: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        self.started()?.process(req).await
    }

    /// Whether the bucket answers. The one implementation with a real one.
    async fn ready(&self) -> bool {
        match self.inner.get() {
            Some(server) => server.ready().await,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoRouter;

    #[async_trait]
    impl ResonateRouter for NoRouter {
        async fn route(
            &self,
            _address: &str,
            _msg: &resonate_core::types::Message,
        ) -> Result<(), Unavailable> {
            unreachable!("nothing is routed")
        }
    }

    fn deps() -> resonate_plugin::ServerDependencies {
        resonate_plugin::ServerDependencies::new(Arc::new(NoRouter) as Arc<dyn ResonateRouter>)
    }

    fn settings(pairs: &[(&str, &str)]) -> resonate_plugin::Configuration {
        let mut loader = resonate_plugin::Loader::new();
        for (k, v) in pairs {
            loader = loader.set(k, v).unwrap();
        }
        loader.load()
    }

    #[test]
    fn a_section_nobody_wrote_gets_this_crate_s_defaults() {
        let config = settings(&[]);
        assert_eq!(config.server(&PLUGIN.id()).key(), "servers.server_blob");
        assert!((PLUGIN.configure)(&config.server(&PLUGIN.id()), deps()).is_ok());
    }

    /// The id the config section is named for, derived from the crate name.
    #[test]
    fn its_id_comes_from_its_crate_name() {
        assert_eq!(PLUGIN.id(), "server_blob");
    }

    #[test]
    fn a_zero_shard_count_is_refused_at_startup() {
        let config = settings(&[("servers.server_blob.timer_shards", "0")]);
        let Err(err) = (PLUGIN.configure)(&config.server(&PLUGIN.id()), deps()) else {
            panic!("zero shards would divide by zero when placing a timer key");
        };
        assert_eq!(err.key, "servers.server_blob.timer_shards");
    }

    #[test]
    fn a_zero_cache_is_refused_at_startup() {
        let config = settings(&[("servers.server_blob.cache_capacity", "0")]);
        assert!((PLUGIN.configure)(&config.server(&PLUGIN.id()), deps()).is_err());
    }

    /// `configure` opens nothing — the contract every plugin's does, and the
    /// reason a bucket that cannot be reached is a startup failure rather than
    /// a request answered wrongly later.
    #[test]
    fn configure_touches_no_bucket() {
        let config = settings(&[
            ("servers.server_blob.bucket", "nonexistent-bucket-xyz"),
            ("servers.server_blob.endpoint", "http://127.0.0.1:1"),
            ("servers.server_blob.allow_http", "true"),
        ]);
        // No network, no credentials, no panic: it is pure extraction.
        assert!((PLUGIN.configure)(&config.server(&PLUGIN.id()), deps()).is_ok());
    }

    /// With no bucket named it runs in memory, so this is the whole lifecycle
    /// without an object store: build, start, answer, stop.
    #[tokio::test]
    async fn it_starts_and_stops_against_the_in_memory_store() {
        let config = settings(&[]);
        let server = (PLUGIN.configure)(&config.server(&PLUGIN.id()), deps()).unwrap();

        assert!(!server.ready().await, "not ready before init");
        server.init(true).await.expect("in-memory needs nothing");
        assert!(server.ready().await, "ready once the store answers");

        server.stop().await.expect("stops cleanly");
    }

    /// `stop` after a failed or absent `init`, which the ABI requires of every
    /// implementation.
    #[tokio::test]
    async fn stop_is_safe_when_init_never_ran() {
        let config = settings(&[]);
        let server = (PLUGIN.configure)(&config.server(&PLUGIN.id()), deps()).unwrap();
        server
            .stop()
            .await
            .expect("nothing to stop is not an error");
    }
}
