//! Encoding benchmark: how should a statement hand its outbound messages back?
//!
//! Measures the whole round trip the server would pay — server-side aggregation,
//! wire transfer in sqlx's binary format, and client decode into the message
//! type — for each candidate encoding at a realistic range of message counts.
//!
//! Run it (release matters — a debug build's decode cost swamps the signal):
//!
//!   BENCH_POSTGRES_URL=postgres://…/bench \
//!     cargo test --release --test encbench -- --nocapture
//!
//! Fixture: `config/postgres/encbench-fixture.sql`.
//!
//! # Result (Postgres 16.13, release build, 5000 paired samples)
//!
//! Median added µs per call over a statement returning no messages. Every
//! encoding decodes to the same `Vec<OutMessage>` with a full `PromiseRecord`.
//!
//! | messages | json   | jsonb  | composite | extra rows | arrays* |
//! |---------:|-------:|-------:|----------:|-----------:|--------:|
//! |        0 |    6.7 |    7.1 |      10.0 |   **-1.4** |    11.6 |
//! |        1 |   52.8 |   64.3 |      37.7 |   **19.2** |    17.6 |
//! |        3 |   64.3 |   83.1 |      60.1 |   **30.6** |    22.9 |
//! |       10 |  145.4 |  183.3 |     215.3 |   **76.1** |    34.0 |
//! |      100 | 1192.8 | 1706.4 |    2090.9 |  **757.4** |   142.1 |
//!
//! \* execute triples plus a bare address list — no promise payload, so not an
//! equivalent encoding. It is the floor, and it is what the twelve
//! single-promise sites actually pay, because there the unblocked promise *is*
//! the statement's own result row and the unblock row carries only an address.
//!
//! Extra rows win at every count, and are free at zero — which is the case that
//! runs before every single operation, since `try_timeout` almost always
//! expires nothing.
//!
//! The mechanism: a row's types are described once, in the `RowDescription` the
//! client caches at prepare time. Every other encoding re-describes each
//! element inline — Postgres' binary composite format repeats `int32 OID,
//! int32 len` per field per element, which for a 14-field message is 11 KB of
//! descriptor across 100 messages, plus per-field OID dispatch in the decoder.
//!
//! jsonb is strictly worse than json (20-43%) for *identical* wire bytes:
//! `jsonb_build_object` builds a parsed binary tree with sorted, deduplicated
//! keys, and output conversion then serialises it back to text for the wire.
//! `json_build_object` concatenates text and stops. For a value that is never
//! stored, indexed, or compared, that round trip buys nothing.

use resonate::core::types::{PromiseRecord, PromiseValue};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::time::Instant;

// Decoded but never inspected: the benchmark measures the cost of producing the
// value, and reading the fields back would measure the reader instead.
#[allow(dead_code, clippy::large_enum_variant)]
#[derive(serde::Deserialize, Debug)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum OutMessage {
    Execute {
        id: String,
        version: i64,
        address: String,
    },
    Unblock {
        address: String,
        promise: PromiseRecord,
    },
}

/// One element of `out_message[]` — the composite encoding decodes flat and is
/// converted to `OutMessage` client-side.
#[allow(dead_code)]
#[derive(sqlx::Type, Debug)]
#[sqlx(type_name = "out_message")]
struct CompositeMsg {
    kind: String,
    address: String,
    task_id: Option<String>,
    version: Option<i32>,
    p_id: Option<String>,
    p_state: Option<String>,
    param_headers: Option<serde_json::Value>,
    param_data: Option<String>,
    value_headers: Option<serde_json::Value>,
    value_data: Option<String>,
    tags: Option<serde_json::Value>,
    timeout_at: Option<i64>,
    created_at: Option<i64>,
    settled_at: Option<i64>,
}

const A_JSON: &str = "
SELECT COALESCE(json_agg(
  CASE WHEN kind='execute'
    THEN json_build_object('kind','execute','id',task_id,'version',version,'address',address)
    ELSE json_build_object('kind','unblock','address',address,'promise',
           json_build_object('id',p_id,'state',p_state,
             'param',json_build_object('data',param_data),
             'value',json_build_object('data',value_data),'tags',tags,
             'timeoutAt',timeout_at,'createdAt',created_at,'settledAt',settled_at))
  END ORDER BY kind, address), '[]'::json) AS messages
FROM enc.msgsrc WHERE grp = $1";

const B_JSONB: &str = "
SELECT COALESCE(jsonb_agg(
  CASE WHEN kind='execute'
    THEN jsonb_build_object('kind','execute','id',task_id,'version',version,'address',address)
    ELSE jsonb_build_object('kind','unblock','address',address,'promise',
           jsonb_build_object('id',p_id,'state',p_state,
             'param',jsonb_build_object('data',param_data),
             'value',jsonb_build_object('data',value_data),'tags',tags,
             'timeoutAt',timeout_at,'createdAt',created_at,'settledAt',settled_at))
  END ORDER BY kind, address), '[]'::jsonb) AS messages
FROM enc.msgsrc WHERE grp = $1";

const C_COMPOSITE: &str = "
SELECT COALESCE(array_agg(ROW(kind,address,task_id,version,p_id,p_state,param_headers,param_data,
                              value_headers,value_data,tags,timeout_at,created_at,settled_at)::enc.out_message
                          ORDER BY kind, address), '{}'::enc.out_message[]) AS messages
FROM enc.msgsrc WHERE grp = $1";

const D_ROWS: &str = "
SELECT kind, address, task_id, version, p_id, p_state, param_headers, param_data,
       value_headers, value_data, tags, timeout_at, created_at, settled_at
FROM enc.msgsrc WHERE grp = $1 ORDER BY kind, address";

/// Execute triples plus a bare address list — valid for the twelve sites where
/// the unblocked promise is already the statement's own result row.
const F_ARRAYS: &str = "
SELECT
  COALESCE(array_agg(task_id ORDER BY task_id) FILTER (WHERE kind='execute'), '{}') AS exec_ids,
  COALESCE(array_agg(version ORDER BY task_id) FILTER (WHERE kind='execute'), '{}') AS exec_versions,
  COALESCE(array_agg(address ORDER BY task_id) FILTER (WHERE kind='execute'), '{}') AS exec_addrs,
  COALESCE(array_agg(address ORDER BY address) FILTER (WHERE kind='unblock'), '{}') AS unblock_addrs
FROM enc.msgsrc WHERE grp = $1";

const E_BASELINE: &str = "SELECT count(*) AS n FROM enc.msgsrc WHERE grp = $1";

/// Paired sampling: every iteration measures the baseline and the variant
/// back to back and keeps the difference, so scheduler drift and background
/// load cancel instead of accumulating. Reports the median paired delta, which
/// is what the encoding actually costs over a statement that returns nothing.
async fn paired<F, Fut>(pool: &PgPool, grp: i32, iters: u32, mut f: F) -> (f64, usize)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = usize>,
{
    for _ in 0..200 {
        f().await;
        let _ = base_call(pool, grp).await;
    }
    let mut deltas: Vec<f64> = Vec::with_capacity(iters as usize);
    let mut n = 0;
    for _ in 0..iters {
        let t0 = Instant::now();
        base_call(pool, grp).await;
        let t1 = Instant::now();
        n = f().await;
        let t2 = Instant::now();
        deltas.push((t2 - t1).as_secs_f64() * 1e6 - (t1 - t0).as_secs_f64() * 1e6);
    }
    deltas.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (deltas[deltas.len() / 2], n)
}

async fn base_call(pool: &PgPool, grp: i32) -> usize {
    let row = sqlx::query(E_BASELINE)
        .bind(grp)
        .fetch_one(pool)
        .await
        .unwrap();
    row.get::<i64, _>("n") as usize
}

async fn bench_json(pool: &PgPool, sql: &'static str, grp: i32, iters: u32) -> (f64, usize) {
    paired(pool, grp, iters, || async {
        let row = sqlx::query(sql).bind(grp).fetch_one(pool).await.unwrap();
        let v: serde_json::Value = row.get("messages");
        let msgs: Vec<OutMessage> = serde_json::from_value(v).unwrap();
        msgs.len()
    })
    .await
}

fn tags_of(v: Option<serde_json::Value>) -> std::collections::HashMap<String, String> {
    v.map(|v| serde_json::from_value(v).unwrap_or_default())
        .unwrap_or_default()
}

fn headers_of(v: Option<serde_json::Value>) -> Option<std::collections::HashMap<String, String>> {
    match v {
        Some(serde_json::Value::Object(m)) if m.is_empty() => None,
        Some(v) => serde_json::from_value(v).ok(),
        None => None,
    }
}

impl From<CompositeMsg> for OutMessage {
    fn from(c: CompositeMsg) -> Self {
        if c.kind == "execute" {
            OutMessage::Execute {
                id: c.task_id.unwrap(),
                version: c.version.unwrap() as i64,
                address: c.address,
            }
        } else {
            OutMessage::Unblock {
                address: c.address,
                promise: PromiseRecord {
                    id: c.p_id.unwrap(),
                    state: c.p_state.unwrap().parse().unwrap(),
                    param: PromiseValue {
                        headers: headers_of(c.param_headers),
                        data: c.param_data,
                    },
                    value: PromiseValue {
                        headers: headers_of(c.value_headers),
                        data: c.value_data,
                    },
                    tags: tags_of(c.tags),
                    timeout_at: c.timeout_at.unwrap(),
                    created_at: c.created_at.unwrap(),
                    settled_at: c.settled_at,
                },
            }
        }
    }
}

async fn bench_composite(pool: &PgPool, grp: i32, iters: u32) -> (f64, usize) {
    paired(pool, grp, iters, || async {
        let row = sqlx::query(C_COMPOSITE)
            .bind(grp)
            .fetch_one(pool)
            .await
            .unwrap();
        let raw: Vec<CompositeMsg> = row.get("messages");
        let msgs: Vec<OutMessage> = raw.into_iter().map(OutMessage::from).collect();
        msgs.len()
    })
    .await
}

async fn bench_rows(pool: &PgPool, grp: i32, iters: u32) -> (f64, usize) {
    paired(pool, grp, iters, || async {
        let rows = sqlx::query(D_ROWS).bind(grp).fetch_all(pool).await.unwrap();
        let msgs: Vec<OutMessage> = rows
            .iter()
            .map(|r| {
                let kind: String = r.get("kind");
                let address: String = r.get("address");
                if kind == "execute" {
                    OutMessage::Execute {
                        id: r.get::<Option<String>, _>("task_id").unwrap(),
                        version: r.get::<Option<i32>, _>("version").unwrap() as i64,
                        address,
                    }
                } else {
                    OutMessage::Unblock {
                        address,
                        promise: PromiseRecord {
                            id: r.get::<Option<String>, _>("p_id").unwrap(),
                            state: r
                                .get::<Option<String>, _>("p_state")
                                .unwrap()
                                .parse()
                                .unwrap(),
                            param: PromiseValue {
                                headers: headers_of(r.get("param_headers")),
                                data: r.get("param_data"),
                            },
                            value: PromiseValue {
                                headers: headers_of(r.get("value_headers")),
                                data: r.get("value_data"),
                            },
                            tags: tags_of(r.get("tags")),
                            timeout_at: r.get::<Option<i64>, _>("timeout_at").unwrap(),
                            created_at: r.get::<Option<i64>, _>("created_at").unwrap(),
                            settled_at: r.get("settled_at"),
                        },
                    }
                }
            })
            .collect();
        msgs.len()
    })
    .await
}

async fn bench_arrays(pool: &PgPool, grp: i32, iters: u32) -> (f64, usize) {
    paired(pool, grp, iters, || async {
        let row = sqlx::query(F_ARRAYS)
            .bind(grp)
            .fetch_one(pool)
            .await
            .unwrap();
        let ids: Vec<String> = row.get("exec_ids");
        let vs: Vec<i32> = row.get("exec_versions");
        let ad: Vec<String> = row.get("exec_addrs");
        let ub: Vec<String> = row.get("unblock_addrs");
        ids.len().max(vs.len()).max(ad.len()) + ub.len()
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn encoding_benchmark() {
    let url = match std::env::var("BENCH_POSTGRES_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("[enc] BENCH_POSTGRES_URL not set — skipped");
            return;
        }
    };
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|c, _| {
            Box::pin(async move {
                sqlx::query("SET search_path TO enc, public")
                    .execute(c)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .unwrap();

    let iters: u32 = std::env::var("BENCH_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    println!(
        "\n[enc] median paired delta over a no-message baseline, µs per call\n      {} samples each, single connection, sqlx binary protocol\n",
        iters
    );
    println!(
        "  {:<8} {:>10} {:>12} {:>12} {:>10} {:>10}",
        "msgs", "json", "jsonb", "composite", "rows", "arrays*"
    );
    println!("  {}", "-".repeat(68));

    for grp in [0i32, 1, 3, 10, 100] {
        let (aj, na) = bench_json(&pool, A_JSON, grp, iters).await;
        let (bj, nb) = bench_json(&pool, B_JSONB, grp, iters).await;
        let (cc, nc) = bench_composite(&pool, grp, iters).await;
        let (dr, nd) = bench_rows(&pool, grp, iters).await;
        let (fa, _) = bench_arrays(&pool, grp, iters).await;
        assert_eq!(na, grp as usize);
        assert_eq!(nb, grp as usize);
        assert_eq!(nc, grp as usize);
        assert_eq!(nd, grp as usize);
        println!(
            "  {:<8} {:>10.1} {:>12.1} {:>12.1} {:>10.1} {:>10.1}",
            grp, aj, bj, cc, dr, fa
        );
    }
    println!("\n  * arrays: execute triples + bare address list — no promise payload,\n    so it is only valid for the twelve sites whose own result row is the\n    unblocked promise. Shown for scale, not as an equivalent encoding.\n");

    // wire size, for the record
    println!("  wire bytes (text form, for scale):");
    for grp in [1i32, 10, 100] {
        let r = sqlx::query("SELECT octet_length(($1)::text) AS j, octet_length(($2)::text) AS jb")
            .bind(
                sqlx::query(A_JSON)
                    .bind(grp)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
                    .get::<serde_json::Value, _>("messages"),
            )
            .bind(
                sqlx::query(B_JSONB)
                    .bind(grp)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
                    .get::<serde_json::Value, _>("messages"),
            )
            .fetch_one(&pool)
            .await
            .unwrap();
        println!(
            "    {:>4} msgs: json {:>7} B   jsonb {:>7} B",
            grp,
            r.get::<i32, _>("j"),
            r.get::<i32, _>("jb")
        );
    }
    println!();
}
