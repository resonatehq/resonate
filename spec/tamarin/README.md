# Tamarin model

A [Tamarin](https://tamarin-prover.github.io/) model of the Resonate protocol,
written against the Lean abstract machine in
[`spec/02-abstract`](../spec/02-abstract),
with an identity-and-capability layer and end-to-end encrypted payloads added
on top, and a set of security properties stated and machine-checked over it.

Three theories. The second exists to justify the first; the third discharges
the part of the first that the prover could not reach whole:

| file | what it is |
|---|---|
| [`models/resonate.spthy`](models/resonate.spthy) | the protocol **with** identity, capabilities and sealed payloads — the reference model |
| [`models/resonate-baseline.spthy`](models/resonate-baseline.spthy) | the same core loop authorised **exactly as `02-abstract/external.lean` authorises it** — the control group |
| [`models/resonate-payload.spthy`](models/resonate-payload.spthy) | the payload envelope on its own, with **the server publishing its entire store** — where confidentiality is proved |
| [`models/resonate-capability.spthy`](models/resonate-capability.spthy) | the task lifecycle on its own, payloads elided — where the capability lemmas are pushed as far as they go |

The baseline is not a straw man. It is the specification's own guard set,
transcribed:

```lean
-- taskFulfill, taskFence, taskSuspend, all three:
if t.state != .acquired then return { status := 409 }
if p.state != .pending  then return { status := 409 }
if t.version != req.version then return { status := 409 }
```

`t.pid` — who claimed the task — appears in none of them. Only `heartbeatOne`
checks it. So in the specification as written, the authority to fence, suspend
and complete a task **is the version number**, and the version number is
published: it is in the `execute` message, in the `task.acquire` response and
in the `task.get` response. The baseline theory makes that concrete and
Tamarin returns the trace.

---

## What the model covers

One rule per transition of the Lean machine, with the same guards, the same
state fields and the same effects.

**External steps** ([`spec/02-abstract/external.lean`](../spec/02-abstract/external.lean)) — `promise.create` for
all four promise kinds plus the idempotent re-create, `promise.settle` with
the `settable` guard and the already-settled no-op, `promise.get`,
`promise.register_callback` and `promise.register_listener` with every branch
(400 / 404 / 422 / register / already-settled), `task.create`, `task.acquire`,
`task.fence` for both the `create` and the `settle` action, `task.suspend` at
arity one and arity two including the `300` branch, `task.complete`
(`taskFulfill`), `task.release`, `task.halt`, `task.continue`,
`task.heartbeat`, `task.get`.

**Internal steps** ([`spec/02-abstract/internal.lean`](../spec/02-abstract/internal.lean)) — `r1` the promise
timeout (timer → `resolved`, otherwise → `rejectedTimedout`, with no value),
`r3` the listener drain that emits `unblock`, `r4` the callback drain and all
three branches of `resumeOne` (resume / buffer / drop), `r5` the lease
timeout, `r6` the retry dispatch that emits `execute`. Schedules are present
as a promise birth site with `expand` as a free function symbol.

One naming note: what the SDK calls `task.complete` is `task.fulfill` on the
wire and `taskFulfill` in `external.lean`. The model uses the specification's
wire tag and the SDK's rule names, and says so at the rule.

Two authorisation paths reach `acquired`, and the model has both:
`task.acquire`, which checks the version, and `task.create` on an id that is
already taken, which does not — `taskCreate` carries no version, so a worker
that knows only the promise id can take a task off the offer queue without
having seen the offer. That is `Server_Task_Create_Adopt`, and the action
`AdoptedWithoutVersion` marks it so the agreement lemma can name it.

**State encoding.** Mutable server state is linear facts, immutable birth
fields are persistent ones — which is the specification's own
`preserved_promise_birth_fields_immutable` made structural:

```
!PromiseMeta(S, pid, kind, tgt, grp, pblob, kblob, creator)   -- birth fields
PromiseSt(S, pid, state, vblob)                               -- mutable
TaskSt(S, tid, state, version, holder)                        -- mutable
Callback(S, awaited, awaiter)   Listener(S, awaited, addr)    -- obligations
ExecSlot(S, tid, msg)  UnblockSlot(S, pid, addr, msg)         -- outbox, keyed
TaskResume(S, tid, ver, awaited)                              -- the resumes ledger
```

Guards are enforced by pattern matching rather than by side conditions: a
`task.complete` quoting the wrong version does not unify with
`TaskSt(S, tid, 'acquired', ver, W)`, which *is* the 409. Versions are
`vsucc`-terms over `'v0'`, so version distinctness is term distinctness and
monotonicity is structural.

## What is abstracted, and why

**Time.** Tamarin terms carry no order relation, so `now`, `timeoutAt`,
`expiresAt` and `retryAt` are not represented. Every step whose only guard is
a deadline comparison — `r1`, `r5`, `r6` — is a rule that may fire at any
point at which its non-temporal guards hold. This is a strict
**over**-approximation: every run of the Lean machine has a counterpart here,
and this theory admits additional runs in which deadlines fire early. Safety
properties proved here hold of the real machine. It enlarges the state space
rather than reducing it.

**The read discipline.** The Lean machine is parametric in `Env.mat`: whether
a read that projects a settled promise also *persists* that settlement. This
model corresponds to `run true`. Under `run false` a read can return a
projected settlement the store has not recorded — but the projected record is
derivable from the promise's public metadata, so it adds no adversary
knowledge, and every store state reachable under `run false` is reachable here
by deferring `Internal_Promise_Timeout`.

**The `resumes` ledger.** `TaskObject.resumes` gates no transition in the
specification; it is read only by `toRecord`, which reports its *length*.
Multiset rewriting cannot clear an unbounded ledger in one step, so resumes
are version-tagged. Clearing on acquisition, which bumps the version, is
therefore exact; clearing on suspension, which does not, leaves a residue
visible only in the reported count.

**`task.suspend` arity.** Given at arity one and arity two, with every branch.
Higher arities are structurally identical — `checkAwaited` is a fold whose
only output is "did any of them settle" and `registerAwaited` is a map. Arity
two is included because it is what shows the all-or-nothing: if *any* awaited
promise has settled the answer is `300` and **nothing** is registered, not even
for the awaited promises still pending.

**Schedules.** `nextCron`, `occurrences` and `expand` are `opaque` in the
specification — calendar arithmetic explicitly outside the protocol, carried
unexercised there. The one thing a schedule contributes is another birth site
for a promise at a server-derived id, and `expand` as a free function symbol
gives exactly the `expand_ids_are_not_forgeable` assumption the specification
flags as load-bearing and unproved.

**Client state compromise** is not modelled: a client is the source of its own
param, so its own compromise leaks it by construction. Long-term key
compromise (`Compromise_Agent`) is modelled for every principal, and worker
compromise (`Compromise_Worker`) additionally surrenders the group key.

---

## The identity and capability layer

Servers, workers and clients are principals with long-term signing keys, and
every request is signed. A task is **offered** by the server to a worker
group; a worker **claims** it with `task.acquire`; on a successful claim the
server mints

```
cap = sign(<'cap', S, tid, version, W>, sk(S))
```

bound to that worker and that version. `task.fence`, `task.suspend`,
`task.complete`, `task.release` and `task.heartbeat` are gated on presenting
that capability **and** on the request being signed by the worker the
capability names. The server checks both, so a token stolen off the wire is
inert — which is the difference between a capability and a bearer token, and
the content of `capability_is_bound_to_its_worker`.

Revocation is the version. `Revoke(S, tid, ver)` is raised wherever a version
stops being live — a lease timeout, a release, a suspension, a halt, a
deadline settlement, and the next claim — and after any of them the capability
naming that version authorises nothing. A worker that was partitioned away and
comes back with a stale capability cannot complete, cannot fence and cannot
suspend. That is `no_write_after_lease_loss`, and it is the fencing theorem.

## The encryption layer

A promise carries

```
pblob = senc(param, ek)
kblob = aenc(<'ek', pid, ek>, pk(gsk(G)))
```

where `ek` is a fresh envelope key chosen by the creator and `gsk(G)` is the
private key of the worker group that will execute the promise. The completing
worker seals its result under the same `ek`. The server stores and forwards
both blobs and holds no key that opens either; the creator keeps `ek`, so it
can read the value that comes back on the `unblock`.

**Why a compromised server is the right way to ask "can the server read it?"**
Everything the server ever holds also crosses the public network, so the
Dolev-Yao adversary already knows every term in the server's store. Adding
`Compromise_Agent` on the server hands it the server's signing key as well. At
that point the adversary knows strictly more than the server does. So a
secrecy lemma that holds *while `LtkReveal(S)` is permitted* says precisely:
the server cannot read it. `param_secrecy` and `value_secrecy` do not exclude
server compromise. They exclude only compromise of the worker group.

One consequence the model makes explicit rather than hides: every worker of a
group holds the same `gsk`, so compromising **any** member exposes every param
and every value ever sealed to that group. `Compromise_Worker` raises
`GroupReveal`, and that is the single exclusion clause in both secrecy lemmas.

---

## Building the tool suite

Ubuntu 24.04, from source, no prebuilt binaries:

```sh
apt-get install -y ghc cabal-install alex happy zlib1g-dev libgmp-dev \
                   build-essential graphviz

# Maude 3.4 -- Tamarin 1.10 rejects Ubuntu's 3.2 as unsupported
curl -O http://archive.ubuntu.com/ubuntu/pool/universe/m/maude/maude_3.4-1_amd64.deb
apt-get install -y ./maude_3.4-1_amd64.deb

git clone --depth 1 --branch 1.10.0 \
    https://github.com/tamarin-prover/tamarin-prover
cd tamarin-prover
cat > cabal.project <<'EOF'
packages: . lib/theory/ lib/term/ lib/utils/ lib/sapic/ lib/export/ lib/accountability/
constraints: crypton < 1.1
EOF
export LANG=C.UTF-8            # happy cannot read the grammar under LANG=C
cabal update && cabal build tamarin-prover -j4
cp "$(cabal list-bin tamarin-prover)" /usr/local/bin/
tamarin-prover test
```

Two things that will otherwise bite you: `crypton >= 1.1` breaks
`yesod-static-1.6.1.3` with a `ByteArrayAccess (Digest MD5)` error, and
`happy` fails with `hGetContents: invalid argument` unless the locale is
UTF-8.

## Running the analysis

```sh
tamarin-prover --prove models/resonate.spthy            # everything
./prove.sh models/resonate.spthy 900                    # lemma at a time, with caps
tamarin-prover interactive models/resonate.spthy        # the GUI, on :3001
```

`prove.sh` proves one lemma per process with a wall-clock and a heap cap, so a
lemma that diverges is reported as such instead of taking the whole run with
it.

The main theory has two configuration switches.

```sh
tamarin-prover --prove -D=FencedSettle models/resonate.spthy
```

removes `Server_Promise_Settle_Targeted` — see the finding below.

```sh
tamarin-prover --prove -D=Reduced models/resonate.spthy
```

removes the eight **response-only** rules: `promise.get`, `task.get`, and the
`*_Idempotent` / `*_Duplicate` / `*_Settled` / `*_Noop` branches where the
specification answers 200 with the record that is already there and writes
nothing. They are modelled in full, and they are expensive: each is a source
for the very linear fact it consumes, which is a cycle in the backward search,
and `promise.get` hands back three opaque blobs, which is what opens the
chains that stall the precomputation.

The reduction is sound for every property here, and the argument is written
out at the rules. In short: (i) they write nothing, so the reachable set of
store states is unchanged; (ii) they add nothing to the adversary — every term
they emit is already on the wire from the request that carried it, and the one
new term, the server's signature over the response, is consumed by no rule in
the theory. A signature nothing accepts extends no attack.

```sh
tamarin-prover --prove -D=Core -D=Reduced models/resonate.spthy
```

is the **analysis configuration**. Unlike `-D=Reduced`, this one drops rules
that genuinely change things, so a result under it is a statement about the
sub-protocol that remains — which is why the twelve rules it drops are listed
by name, with what is lost, in the theory's section 0. They are: schedules;
the `task.create` family, which is a second route to `acquired` and a second
sealed-payload delivery path; timer promises; standalone
`promise.register_callback`, which `task.suspend` still exercises; and
`task.suspend` at arity two.

The reason is the sealed payload. A worker has to **deconstruct** what the
server passed through as an unconstrained variable, and every delivery path
that does so opens chains the source refinement has to resolve; on a 16 GB
host it does not converge for the full surface. `-D=Core` leaves exactly one
such path — `task.acquire` → `Worker_Start` — plus the `unblock` that
delivers a value to a listener. That is the least this protocol can be
modelled with and still be this protocol.

The full theory remains the reference: it is what says what Resonate *is*, and
it loads and checks clean. `-D=Core` is what the machine could get through.

---

## The properties

### A. Executability

These say the model runs, and therefore that the rest of the lemmas are not
about an empty set of traces.

| lemma | statement |
|---|---|
| `exec_full_cycle` | client creates a targeted promise and listens; server offers; worker claims, opens the param, completes with a sealed value; server unblocks; client opens the value — nobody compromised |
| `exec_cycle_under_compromised_server` | the same round trip **with the server's key in the adversary's hands**, and the param and value still secret |
| `exec_suspend_and_resume` | worker parks on an external promise, it settles, the callback drains, the task goes back on offer with the wake recorded |
| `exec_fenced_create` | a worker holding a lease spawns a child invocation, and the child goes on offer in its own right |
| `exec_lease_handover` | a worker loses its lease and a *different* worker finishes the work at a higher version |

### B. Confidentiality

| lemma | statement |
|---|---|
| `param_secrecy` | a param is known to the adversary only if the worker group was compromised — server compromise is **not** excluded |
| `value_secrecy` | the same for values |
| `param_read_only_by_group_member` | only a member of the group a promise was sealed to ever holds its param in clear |

### C. Capability

| lemma | statement |
|---|---|
| `capability_use_requires_claim` | every fence, suspend, complete, release and heartbeat was preceded by a successful `task.acquire` by that worker at that version |
| `capability_is_bound_to_its_worker` | a capability granted to `W1` cannot be used by anyone else — stealing the token is not enough |
| `claim_is_exclusive` | two workers never hold the same `<task, version>` |
| `no_write_after_lease_loss` | **the fencing theorem** — after a revocation, the capability naming that version authorises nothing |
| `completion_is_by_the_claimant` | a completion at version `v` is by the worker that claimed `v` |
| `no_forged_capability` | a worker's capability came from the server, not from the network |
| `claim_agreement` | the server records as holder only a worker that actually asked |

### D. Integrity

| lemma | statement |
|---|---|
| `settlement_is_unique` | a promise settles once and stays settled |
| `offer_authentication` | a worker starts work only on a task the server actually offered |
| `unblock_authentication` | an `unblock` a client acts on was really sent by the server for a promise that had really settled |
| `value_provenance` | a value a client accepts was produced by a lease holder, or the promise settled without one |

### E. The confidentiality result, and what it cost

`models/resonate-payload.spthy` is the envelope, the group key and the server
as courier, with the task lifecycle left out. It is **not** a reduction of
`resonate.spthy` and must not be read as one — it says nothing about fencing
or capabilities. What it does say, it says in the strongest form available:

> **the server publishes its entire store.**

There is no server principal in it and no server key. Every term the server
would hold — the sealed param, the sealed envelope key, the sealed value —
goes straight to the network the moment it arrives. Requests are
unauthenticated too, which only adds behaviour. A secrecy result under that
assumption is stronger than one under a compromised server: a compromised
server has its keys and its store, and here the store *is* the adversary's.

All six lemmas verify. Two of them only after the prover broke them first:

**A param blob replayed as a value blob.** Param and value were sealed under
the same envelope key with nothing to tell them apart, so the creator accepted
its *own param* back as the answer to its promise — replayed off the wire as
an `unblock`, with no worker having run at all. Four steps. Sealing under one
key is not enough; what is sealed has to say what it is. The envelope now
carries `'param'` and `'value'` tags:

```
pblob = senc(<'param', param>, ek)
vblob = senc(<'value', value>, ek)
```

and the replay no longer types. The fix is in `resonate.spthy` too.

**`value_secrecy` was overclaimed.** It asserted that *every* value stays
secret, including one belonging to a promise the adversary created for itself
— but the creator of a promise chooses its envelope key, so reading the result
is the design, not a break. That lemma and `param_read_only_by_group_member`
are now scoped to promises an honest creator sealed.

### F. The finding in the specification

`promiseSettle` in `02-abstract/external.lean` checks `settable` and that the
promise is pending. It checks no version, no lease and no identity. So for a
**targeted** promise — one a worker holds a lease on — any principal that
knows the id can settle it out from under that worker, and `setSettled` will
fulfil the task in the same step. The capability layer of this theory does not
close that door, because the door is not on the task API.

`attack_open_settle_preempts_the_holder` is an **exists-trace**, so Tamarin
returns the attack as a witness, with nobody compromised in it. Building with
`-D=FencedSettle` drops `Server_Promise_Settle_Targeted`, and the all-traces
lemma `settlement_of_targeted_is_fenced` takes its place: a targeted promise
is then settled only by the worker holding its lease or by a parent
invocation fencing a settle on it. That pair is the statement of the fix.

## Results

See [`RESULTS.md`](RESULTS.md).
