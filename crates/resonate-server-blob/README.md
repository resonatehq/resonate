# resonate-server-blob

A technical reference for the blob backend: a complete `ResonateServer` over
conditional-write object storage, one CAS'd object per origin.

## The document format

One origin — everything before the first `':'` of an id — is one object. That
object holds the origin's entire state: every promise, every task, every
registration, and every armed deadline. A single conditional write therefore
commits any single-origin transition atomically, which is every transition
this server accepts.

### The key

```
<prefix>wf/<origin>
```

`<prefix>` is the deployment's configured key prefix (possibly empty, always
`/`-terminated when not). The origin is percent-encoded into the key:
`A–Z a–z 0–9 . -` pass through, every other byte becomes `%XX` with uppercase
hex. The origin `order-7` lives at `wf/order-7`; the origin `テスト` lives at
`wf/%E3%83%86%E3%82%B9%E3%83%88`.

The origin appears in the body zero times — every id is stored relative to it
— but the header's `og` field binds the object to its key (see below), so a
misrouted read or a misdirected write is refused rather than silently
corrupting another workflow.

### The body

One line of canonical JSON per entity, joined by `\n`, no trailing newline.
Every line is an object whose first field `t` names its type:

| `t` | line | one per |
|---|---|---|
| `"h"` | header | document (always the first line) |
| `"p"` | promise | promise, sorted by id |
| `"k"` | task | task, sorted by id |
| `"pt"` | armed promise deadline | pending promise carrying `resonate:target`, sorted by `(dl, id)` |
| `"kt"` | armed task deadline | armed retry or lease, sorted by `(dl, id, kind)` |

A document written by this encoder, read back by this decoder, is the same
document exactly. The example below is real encoder output — an origin
`order-7` holding a dispatched task promise (`charge`, acquired by worker
`p1`), an external promise with an awaiter and a listener (`paid`), and a
settled promise (`done`):

```
{"t":"h","v":1,"clk":1500,"g":6,"og":"dee47a9f34bede3b","ta":61000}
{"t":"p","id":"charge","st":0,"tg":{"resonate:target":"http://worker:9999"},"pm":{"d":"aGk="},"to":900000,"ca":1000}
{"t":"p","id":"done","st":1,"vl":{"h":{"x":"1"},"d":"b2s="},"to":500000,"ca":1400,"sa":1500}
{"t":"p","id":"paid","st":0,"tg":{"resonate:external":"true"},"to":600000,"ca":1100,"cb":["charge"],"ls":["poll://any@group"]}
{"t":"k","id":"charge","st":1,"v":1,"pid":"p1","ttl":60000}
{"t":"pt","dl":900000,"id":"charge"}
{"t":"kt","dl":61000,"id":"charge","k":1}
```

### Header (`"h"`)

| field | type | meaning |
|---|---|---|
| `v` | int | Format version, currently `1`. A document with a higher `v` is refused rather than misread. |
| `clk` | int | The origin's high-water `now`, ms. Never decreases, so the origin's view of time is monotone even if a caller's clock regresses. |
| `g` | int | Generation — bumped by the shell once per committed write. Diagnostic only. |
| `og` | string | 16 lowercase hex chars of 64-bit FNV-1a over the origin string. A document whose `og` does not hash the origin it was read under is refused (`OriginMismatch`). |
| `ta` | int | Deadline of the timer object currently armed for this origin — the minimum of every `pt`/`kt` deadline. Omitted when nothing is armed. |

### Promise (`"p"`)

| field | type | meaning |
|---|---|---|
| `id` | string | Relative to the origin: `""` for the origin's own promise, the lineage after the first `':'` otherwise. `order-7:charge` is stored as `"charge"`. |
| `st` | int | State: `0` pending, `1` resolved, `2` rejected, `3` rejected_canceled, `4` rejected_timedout. |
| `tg` | map | Tags, keys sorted. Omitted when empty. |
| `pm` | payload | Param. Omitted when empty. |
| `vl` | payload | Value. Omitted when empty. |
| `to` | int | `timeoutAt`, ms. |
| `ca` | int | `createdAt`, ms. |
| `sa` | int | `settledAt`, ms. Omitted while pending. |
| `cb` | array | Callback awaiter ids (relative), in registration order — order is protocol-visible: a settlement fans out to awaiters in the order they registered. Omitted when empty. |
| `ls` | array | Listener addresses, in registration order, unique. Not ids — never relativized. Omitted when empty. |

A payload is `{"h":{...},"d":"..."}` — headers (keys sorted) and data, each
half omitted when absent, so an empty payload is `{}` and is itself omitted.

### Task (`"k"`)

A task's id *is* its promise's id, so `k` lines join to `p` lines by `id`.

| field | type | meaning |
|---|---|---|
| `id` | string | Relative id, as above. |
| `st` | int | State: `0` pending, `1` acquired, `2` suspended, `3` halted, `4` fulfilled. |
| `v` | int | Version — the fencing token every task operation is checked against. |
| `pid` | string | Owning process while acquired. Omitted otherwise. |
| `ttl` | int | Lease length, ms. Omitted when absent. |
| `rs` | array | Awaited promise ids (relative) whose settlement this task has not yet observed — `resumes` on the wire is this set's size. Omitted when empty. |

### Armed deadlines (`"pt"`, `"kt"`)

The document's own timeout tables — what the SQL backends keep in
`promise_timeouts` and `task_timeouts`.

| field | type | meaning |
|---|---|---|
| `dl` | int | Deadline, ms. |
| `id` | string | Relative id. |
| `k` | int | `kt` only — `0` retry (task pending, awaiting re-dispatch), `1` lease (task acquired, lease expiry). |

A `pt` line exists only for a pending promise carrying `resonate:target`: an
undispatched promise has nothing to notify, so its expiry is applied lazily on
read. Each kind is sorted by `(dl, id)`, so **the first line of each kind is
that kind's minimum armed deadline** — recovery can find the next deadline
without parsing the whole document.

### Canonical encoding

Encoding is a function of the state alone: two encoders given equal documents
produce identical bytes. The rules —

- ASCII only; anything else is `\uXXXX` with lowercase hex, supplementary
  planes as surrogate pairs, `/` never escaped;
- minimal escapes, integers with no exponent, no insignificant whitespace;
- fixed key order within every line, fixed line order within the document,
  map keys sorted by code unit;
- omission, never `null` / `[]` / `false` / `{}`, for anything empty.

This is load-bearing, not cosmetic: the applier compares bytes to decide
whether a write is needed at all (*if the decision changed nothing, write
nothing*), and a writer recognizes its own landed write after a lost response
by re-encoding and comparing.

### Evolution

The decoder skips unknown line types and unknown fields. That, plus
omission-when-empty, is the whole compatibility story: a newer server may add
both without breaking an older reader, and an older document never presents a
field the newer reader cannot default. What is *not* tolerated: a header `v`
above the reader's own (refused as `UnsupportedVersion`), a body that is not
the shape a document has (`Malformed`), and an `og` that does not hash the key
(`OriginMismatch`).

The codec lives in [`src/codec.rs`](src/codec.rs); the document types it
serializes are [`src/kernel/state.rs`](src/kernel/state.rs).

## The document in memory, and the transaction over it

### The types

The decoded form of a document is `OriginDoc`
([`src/kernel/state.rs`](src/kernel/state.rs)):

```rust
pub struct OriginDoc {
    pub promises: BTreeMap<String, PromiseDoc>,  // by FULL promise id
    pub tasks:    BTreeMap<String, TaskDoc>,     // by full promise id — a task's id IS its promise's id
    pub clock:    i64,          // high-water `now`; never decreases
    pub gen:      u64,          // bumped by the shell per committed write; diagnostic
    pub timer_at: Option<i64>,  // deadline of the timer object armed for this origin
}

pub struct PromiseDoc {
    pub state:      PromiseState,
    pub param:      PromiseValue,
    pub value:      PromiseValue,
    pub tags:       BTreeMap<String, String>,
    pub timeout_at: i64,
    pub created_at: i64,
    pub settled_at: Option<i64>,
    pub callbacks:  Vec<String>,   // awaiter ids, registration order (protocol-visible)
    pub listeners:  Vec<String>,   // addresses, registration order, unique
}

pub struct TaskDoc {
    pub state:    TaskState,
    pub version:  i64,             // the fencing token
    pub pid:      Option<String>,
    pub ttl:      Option<i64>,
    pub resumes:  BTreeSet<String>,
    pub retry_at: Option<i64>,     // timeout_type = 0, armed while pending
    pub lease_at: Option<i64>,     // timeout_type = 1, armed while acquired
}
```

The mapping to the wire format is almost 1:1, with two folds and two
derivations. The `"kt"` deadline lines are not separate state — they are
`TaskDoc.retry_at` / `lease_at` folded into the task, and `check_invariants`
holds them to *at most one armed*. The `"pt"` lines are **derived** on encode
from `PromiseDoc::timeout_armed()` (pending + carrying `resonate:target`), not
stored. `timer_at` is likewise derived — the kernel maintains it as
`min_deadline(&doc)` — and `PromiseDoc` deliberately derives `PartialEq`,
because the write law below is a whole-document comparison.

`BTreeMap`, not `HashMap`, everywhere: iteration order is protocol-visible
(`preload` is ordered by id), encode order is canonical, and a seeded
differential run must reproduce its trajectory.

### The transaction: `Tx`

`Tx` ([`src/kernel/handle.rs`](src/kernel/handle.rs)) is the kernel's
transaction metaphor — a snapshot mutated freely, turned into effects at the
end:

```rust
pub(crate) struct Tx {
    pub(crate) doc:   OriginDoc,               // a CLONE of the input document
    pub(crate) sends: Vec<(String, OutEntry)>, // messages the decision owes, outside the doc
}
```

The roll-up happens in three nested layers.

**Per request.** `handle(doc, req, now, cfg)` does `Tx::new(doc)` — a full
clone — and dispatches to the one `op_*` for the request kind. Every mutation
the op makes lands on `tx.doc` (insert a promise, flip a task state, push a
callback); messages go to `tx.sends`. Then `tx.finish(before)` recomputes
`timer_at = min_deadline(&tx.doc)` and linearizes the outcome into effects
**in the order the shell must perform them**:

1. `SetTimeout` — arm the new timer first, so the deadline is covered even if
   the process dies mid-commit;
2. `SetDocument(tx.doc)` — the whole mutated clone. There is no diff; the
   document is the write unit;
3. `DelTimeout` — the old timer, only after the commit it belonged to is gone;
4. `Send { address, out }` — strictly post-commit.

One wrinkle worth knowing: a request that returns a 4xx still returns
effects. `try_timeout` runs first in most ops and lazily settles expired
promises it walked past, so even a rejected request can commit the
expirations it observed — which is also why reads go through the applier
rather than a separate read path.

**Per batch.** The applier's `decide()` ([`src/applier.rs`](src/applier.rs))
folds a mailbox batch through the kernel sequentially: clone the loaded
document, and for each work item run `handle` (or `drain`, for a tick), then
`apply_effects(&mut doc, &fx)` — which takes the `SetDocument` payload as the
new working document — so *request k sees request k−1's document*. The clock
folds monotonically here: `now = work.now().max(clock)`, so a caller with a
regressed clock cannot un-expire anything. Sends accumulate across the batch;
`decide` itself is pure — no I/O, no clock reads.

**Per commit.** `perform()` encodes the decided document and applies the
write law: if `changed()` finds promises, tasks and `timer_at` all equal —
`clock` and `gen` are deliberately excluded, or every read would become a
write — nothing is written at all. Otherwise: one conditional PUT of the
whole document against the etag it was loaded at, so a hot origin costs one
CAS per *batch* rather than one per request. Losing the race means the batch
is re-decided against the freshly loaded document, up to `max_cas_retries`
times before the caller is told there is no answer.
