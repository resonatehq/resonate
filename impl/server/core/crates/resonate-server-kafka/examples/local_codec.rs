//! How much does loading an origin from the local store cost, per format?
//!
//! The local RocksDB copy is internal and rebuildable, so its format is free
//! to choose. This measures, for origins of 1 to 1000 promises, the cost of
//! turning stored records into the `OriginDoc` the kernel decides over:
//!
//! - `json`     — the blob codec's lines, what the store holds today
//! - `postcard` — serde, compact binary
//! - `rkyv`     — zero-copy archive, validated, then deserialized
//! - `rkyv!`    — the same, unvalidated (the store is ours)
//!
//! plus two reference points: `rkyv read` (touch every field in place, build
//! nothing — the zero-copy floor) and `clone` (copy a decoded `OriginDoc` —
//! what the kernel already pays per decision, and what a cache of decoded
//! documents costs instead of a decode).
//!
//!   cargo run --release -p resonate-server-kafka --example local_codec

use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::time::{Duration, Instant};

use resonate_core::types::{PromiseState, PromiseValue, TaskState};
use resonate_server_blob::kernel::state::{OriginDoc, PromiseDoc, TaskDoc};
use resonate_server_kafka::record;

// --- a mirror of the kernel's types, with the derives the formats need -----

#[derive(
    serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
struct P {
    st: u8,
    pm_h: Option<Vec<(String, String)>>,
    pm_d: Option<String>,
    vl_h: Option<Vec<(String, String)>>,
    vl_d: Option<String>,
    tags: Vec<(String, String)>,
    to: i64,
    ca: i64,
    sa: Option<i64>,
    cb: Vec<String>,
    ls: Vec<String>,
    task: Option<T>,
}

#[derive(
    serde::Serialize, serde::Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
struct T {
    st: u8,
    v: i64,
    pid: Option<String>,
    ttl: Option<i64>,
    rs: Vec<String>,
    retry: Option<i64>,
    lease: Option<i64>,
}

const PSTATES: [PromiseState; 5] = [
    PromiseState::Pending,
    PromiseState::Resolved,
    PromiseState::Rejected,
    PromiseState::RejectedCanceled,
    PromiseState::RejectedTimedout,
];
const TSTATES: [TaskState; 5] = [
    TaskState::Pending,
    TaskState::Acquired,
    TaskState::Suspended,
    TaskState::Halted,
    TaskState::Fulfilled,
];

fn headers(h: &Option<std::collections::HashMap<String, String>>) -> Option<Vec<(String, String)>> {
    h.as_ref().map(|h| {
        let mut v: Vec<_> = h.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        v.sort();
        v
    })
}

fn to_mirror(p: &PromiseDoc, t: Option<&TaskDoc>) -> P {
    P {
        st: PSTATES.iter().position(|s| *s == p.state).unwrap() as u8,
        pm_h: headers(&p.param.headers),
        pm_d: p.param.data.clone(),
        vl_h: headers(&p.value.headers),
        vl_d: p.value.data.clone(),
        tags: p.tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        to: p.timeout_at,
        ca: p.created_at,
        sa: p.settled_at,
        cb: p.callbacks.clone(),
        ls: p.listeners.clone(),
        task: t.map(|t| T {
            st: TSTATES.iter().position(|s| *s == t.state).unwrap() as u8,
            v: t.version,
            pid: t.pid.clone(),
            ttl: t.ttl,
            rs: t.resumes.iter().cloned().collect(),
            retry: t.retry_at,
            lease: t.lease_at,
        }),
    }
}

fn from_mirror(m: P) -> (PromiseDoc, Option<TaskDoc>) {
    let value = |h: Option<Vec<(String, String)>>, d| PromiseValue {
        headers: h.map(|h| h.into_iter().collect()),
        data: d,
    };
    (
        PromiseDoc {
            state: PSTATES[m.st as usize],
            param: value(m.pm_h, m.pm_d),
            value: value(m.vl_h, m.vl_d),
            tags: m.tags.into_iter().collect(),
            timeout_at: m.to,
            created_at: m.ca,
            settled_at: m.sa,
            callbacks: m.cb,
            listeners: m.ls,
        },
        m.task.map(|t| TaskDoc {
            state: TSTATES[t.st as usize],
            version: t.v,
            pid: t.pid,
            ttl: t.ttl,
            resumes: t.rs.into_iter().collect(),
            retry_at: t.retry,
            lease_at: t.lease,
        }),
    )
}

// --- a realistic origin --------------------------------------------------------

/// An origin of `n` promises as an SDK leaves it mid-run: a root with a
/// task, children with the SDK's lineage tags and a 256-byte payload, half of
/// them settled, a third of them dispatched to a worker.
fn origin(n: usize) -> OriginDoc {
    let mut doc = OriginDoc::default();
    let data = "A".repeat(256);
    for i in 0..n {
        let id = if i == 0 {
            "wf-1".to_string()
        } else {
            format!("wf-1:step-{i}")
        };
        let mut tags = BTreeMap::new();
        for k in [
            "resonate:origin",
            "resonate:root",
            "resonate:parent",
            "resonate:scope",
        ] {
            tags.insert(k.to_string(), "wf-1".to_string());
        }
        let targeted = i % 3 == 0;
        if targeted {
            tags.insert("resonate:target".into(), "poll://any@workers".into());
        }
        let settled = i % 2 == 1;
        doc.promises.insert(
            id.clone(),
            PromiseDoc {
                state: if settled {
                    PromiseState::Resolved
                } else {
                    PromiseState::Pending
                },
                param: PromiseValue {
                    headers: None,
                    data: Some(data.clone()),
                },
                value: PromiseValue {
                    headers: None,
                    data: settled.then(|| data.clone()),
                },
                tags,
                timeout_at: 1_000_000 + i as i64,
                created_at: 1_000 + i as i64,
                settled_at: settled.then_some(2_000 + i as i64),
                callbacks: if !settled && i > 0 {
                    vec!["wf-1".to_string()]
                } else {
                    vec![]
                },
                listeners: if i == 0 {
                    vec!["poll://any@client".to_string()]
                } else {
                    vec![]
                },
            },
        );
        if targeted {
            let fulfilled = settled;
            doc.tasks.insert(
                id,
                TaskDoc {
                    state: if fulfilled {
                        TaskState::Fulfilled
                    } else {
                        TaskState::Acquired
                    },
                    version: 2,
                    pid: (!fulfilled).then(|| "worker-7".to_string()),
                    ttl: (!fulfilled).then_some(60_000),
                    resumes: BTreeSet::new(),
                    retry_at: None,
                    lease_at: (!fulfilled).then_some(61_000 + i as i64),
                },
            );
        }
    }
    doc.timer_at = resonate_server_blob::kernel::state::min_deadline(&doc);
    doc
}

// --- measuring ------------------------------------------------------------------

/// Mean time per call, over at least `budget`.
fn time(budget: Duration, mut f: impl FnMut()) -> Duration {
    f(); // warm
    let start = Instant::now();
    let mut n = 0u32;
    while start.elapsed() < budget {
        f();
        n += 1;
    }
    start.elapsed() / n
}

fn fmt(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns >= 1_000_000 {
        format!("{:.2} ms", ns as f64 / 1e6)
    } else if ns >= 1_000 {
        format!("{:.1} µs", ns as f64 / 1e3)
    } else {
        format!("{ns} ns")
    }
}

fn main() {
    let budget = Duration::from_millis(400);
    println!(
        "{:>6}  {:>10} {:>10} {:>10} {:>10}  {:>10} {:>10}   bytes json/postcard/rkyv",
        "size", "json", "postcard", "rkyv", "rkyv!", "rkyv read", "clone"
    );
    for n in [1usize, 10, 100, 1_000] {
        let doc = origin(n);
        let ids: Vec<&String> = doc.promises.keys().collect();

        let json: Vec<(String, Vec<u8>)> = ids
            .iter()
            .map(|id| {
                let p = &doc.promises[*id];
                (
                    (*id).clone(),
                    record::encode_promise(id, p, doc.tasks.get(*id)),
                )
            })
            .collect();
        let post: Vec<(String, Vec<u8>)> = ids
            .iter()
            .map(|id| {
                let m = to_mirror(&doc.promises[*id], doc.tasks.get(*id));
                ((*id).clone(), postcard::to_allocvec(&m).unwrap())
            })
            .collect();
        // As RocksDB hands them back: plain, possibly unaligned, byte vectors.
        let rk: Vec<(String, Vec<u8>)> = ids
            .iter()
            .map(|id| {
                let m = to_mirror(&doc.promises[*id], doc.tasks.get(*id));
                (
                    (*id).clone(),
                    rkyv::to_bytes::<rkyv::rancor::Error>(&m).unwrap().to_vec(),
                )
            })
            .collect();

        let t_json = time(budget, || {
            let d = record::assemble(json.iter().map(|(id, v)| (id.clone(), &v[..]))).unwrap();
            black_box(d);
        });
        let assemble = |decode: &dyn Fn(&[u8]) -> P, recs: &[(String, Vec<u8>)]| {
            let mut d = OriginDoc::default();
            for (id, bytes) in recs {
                let (p, t) = from_mirror(decode(bytes));
                if let Some(t) = t {
                    d.tasks.insert(id.clone(), t);
                }
                d.promises.insert(id.clone(), p);
            }
            d.timer_at = resonate_server_blob::kernel::state::min_deadline(&d);
            d
        };
        let t_post = time(budget, || {
            black_box(assemble(&|b| postcard::from_bytes::<P>(b).unwrap(), &post));
        });
        let aligned = |b: &[u8]| {
            let mut v = rkyv::util::AlignedVec::<16>::new();
            v.extend_from_slice(b);
            v
        };
        let t_rkyv = time(budget, || {
            black_box(assemble(
                &|b| {
                    let a = aligned(b);
                    let r = rkyv::access::<ArchivedP, rkyv::rancor::Error>(&a).unwrap();
                    rkyv::deserialize::<P, rkyv::rancor::Error>(r).unwrap()
                },
                &rk,
            ));
        });
        let t_rkyv_unchecked = time(budget, || {
            black_box(assemble(
                &|b| {
                    let a = aligned(b);
                    let r = unsafe { rkyv::access_unchecked::<ArchivedP>(&a) };
                    rkyv::deserialize::<P, rkyv::rancor::Error>(r).unwrap()
                },
                &rk,
            ));
        });
        let t_read = time(budget, || {
            let mut sum = 0i64;
            for (_, b) in &rk {
                let a = aligned(b);
                let r = unsafe { rkyv::access_unchecked::<ArchivedP>(&a) };
                sum += r.to.to_native()
                    + r.tags.len() as i64
                    + r.pm_d.as_ref().map_or(0, |d| d.len() as i64);
            }
            black_box(sum);
        });
        let t_clone = time(budget, || {
            black_box(doc.clone());
        });

        let bytes = |r: &[(String, Vec<u8>)]| r.iter().map(|(_, v)| v.len()).sum::<usize>();
        println!(
            "{:>6}  {:>10} {:>10} {:>10} {:>10}  {:>10} {:>10}   {}/{}/{}",
            n,
            fmt(t_json),
            fmt(t_post),
            fmt(t_rkyv),
            fmt(t_rkyv_unchecked),
            fmt(t_read),
            fmt(t_clone),
            bytes(&json),
            bytes(&post),
            bytes(&rk),
        );
    }
}
