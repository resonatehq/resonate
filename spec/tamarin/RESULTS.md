# Analysis results

Tool suite as built in this repository's README: **Tamarin 1.10.0**, **Maude
3.4**, GHC 9.4.7, GraphViz 2.43. Host: 4 cores, 16 GB.

Every lemma is proved in its own process, with a wall-clock cap and a heap cap
(`./prove.sh`), so a lemma that diverges is reported as such rather than taking
the run with it.

## Summary

| theory | what it establishes | result |
|---|---|---|
| [`resonate-payload.spthy`](models/resonate-payload.spthy) | params and values are unreadable by the server | **6 / 6 verified** |
| [`resonate-baseline.spthy`](models/resonate-baseline.spthy) | why the identity and capability layer is needed | **3 / 5**, and the three are the substantive ones |
| [`resonate-capability.spthy`](models/resonate-capability.spthy) | the claim is what authorises | 5 / 16, two of them unconditional |
| [`resonate.spthy`](models/resonate.spthy) | the whole protocol | 4 / 26, all four conditional |

---

## Running Tamarin on these models

**exists-trace lemmas need breadth-first search.** The model has rules that
read state without changing it — `promise.get`, `task.get`, the retry dispatch
— and each is a *source* for the very linear fact it consumes. Depth-first
search, the default, walks into those cycles and does not come back.
Breadth-first finds the witness. `prove.sh` tries depth-first on a short budget
and falls back to `--stop-on-trace=BFS`; neither order dominates, and one
lemma took 8 steps under DFS and 729 under BFS.

**The opaque payloads need a sources lemma, and which one matters.** The server
stores `pblob` and `kblob` as terms it never looks inside — that *is* the
design — so Tamarin cannot trace where the cleartext a worker decrypts came
from: the chain runs `In` → `!PromiseMeta` → `Out` → `In` → decryption with an
unconstrained variable in the middle. Without help:

```
[Open Chains] Too many chain constraints, stopping precomputation.
```

and the run climbs past 13 GB without converging. `sources_param` and
`sources_value`, stated over the cleartexts, are **not enough**. The
deconstruction that actually opens the chains recovers the **envelope key**:

```
lemma sources_envelope [sources]:
  "All ek tid #i. Unseal(ek, tid) @ #i
     ==> (Ex #j. KU(ek) @ #j & #j < #i)
       | (Ex pid G #j. Seal(ek, pid, G) @ #j & #j < #i)"
```

**Rules that answer without writing are expensive and can be dropped soundly.**
`-D=Reduced` removes the eight of them — `promise.get`, `task.get`, and the
`*_Idempotent` / `*_Duplicate` / `*_Settled` / `*_Noop` branches. They write
nothing, so the reachable store states are unchanged; and they emit no term the
adversary cannot already derive, since every blob is on the wire from the
request that carried it and the one new term, the server's signature over the
response, is consumed by no rule. A signature nothing accepts extends no attack.

---

## Confidentiality — `models/resonate-payload.spthy`

The payload envelope on its own: the sealed param, the group key, and the
server as courier. Not a reduction of `resonate.spthy` and not to be read as
one — it omits the task lifecycle entirely, so it says nothing about fencing or
capabilities. What it does say, it says in the strongest form available: **the
server publishes its entire store.** No server principal, no server key; every
term the server would hold goes straight to the network. Requests are
unauthenticated too, which only adds behaviour.

| lemma | result |
|---|---|
| `sources_envelope` | **verified (12 steps)** |
| `exec_round_trip` | **verified (14 steps)** |
| `param_secrecy` | **verified (8 steps)** |
| `value_secrecy` | **verified (9 steps)** |
| `param_read_only_by_group_member` | **verified (16 steps)** |
| `value_reaches_its_creator` | **verified (18 steps)** |

Params and values are readable by the worker group and by the creator that
minted the envelope key, and by nobody else — including a server that has
published everything it ever held. The only way in is the group key, whether it
leaks on its own or through a member holding it; both raise `GroupReveal`,
which is the single exclusion clause in both secrecy lemmas.

### Two defects the prover found

**A param blob replayed as a value blob.** `value_reaches_its_creator` was
falsified in four steps. Param and value were sealed under the same envelope
key with nothing to tell them apart, so the creator accepted its *own param*
back as the answer to its promise — replayed off the wire as an `unblock`, with
no worker having run at all. Sealing under one key is not enough; what is
sealed has to say what it is. The envelope now reads

```
pblob = senc(<'param', param>, ek)
vblob = senc(<'value', value>, ek)
```

and the replay no longer types. The fix is in `resonate.spthy` as well.

**`value_secrecy` was overclaimed.** Falsified in seven steps, and correctly
so: it asserted that every value stays secret, including one belonging to a
promise the adversary created for itself. But the creator of a promise chooses
its envelope key, so reading the result is the design. That lemma and
`param_read_only_by_group_member` are now scoped to promises an honest creator
sealed, and both verify.

---

## Baseline — the specification's own authorisation

`models/resonate-baseline.spthy`, no flags. 4 GB heap, 600 s per lemma.

| lemma | expected | result |
|---|---|---|
| `attack_param_is_public` | found | **verified (5 steps)** |
| `attack_outsider_completes_a_held_task` | found | **verified (8 steps)** |
| `completion_is_by_the_claimant` | falsified | **falsified — found trace (8 steps)** |
| `exec_cycle` | found | timeout (600 s, DFS and BFS) |
| `no_write_after_lease_loss` | verified | timeout (2400 s, three rankings) |

The three that matter landed, and nobody is compromised in any of them:

- **`attack_param_is_public` — the server reads the param, and so does anyone
  else.** `promise.create` carries a `Value` record in the clear, `promise.get`
  hands it back, and the `task.acquire` response hands it to whoever claimed
  the task. Compare `param_secrecy` above, which holds even with the server's
  whole store published.

- **`attack_outsider_completes_a_held_task` — a principal that never claimed
  the task completes it.** `task.get` is ungated and reports
  `TaskRecord.version`; the version is the whole of the authority `taskFulfill`
  asks for. This is the trace the identity and capability layer exists to rule
  out.

- **`completion_is_by_the_claimant` — false here.** The statement is "a
  completion the server accepts came from the worker holding the lease". A
  `task.fulfill` request carries no identity to compare against `t.pid`, so the
  server cannot tell, and Tamarin returns the counterexample. The same lemma is
  verified on the capability theory.

---

## Capability — `models/resonate-capability.spthy`

The task lifecycle with the envelope removed: a promise's param and value are
public constants, so this theory says nothing about confidentiality. It carries
the whole authorisation story with the specification's guards unchanged, plus
the two the identity model adds. 8 GB heap, 400 s per lemma.

| lemma | result | stands on |
|---|---|---|
| `grant_is_a_claim` | **verified (2 steps)** | nothing — holds with no hypotheses |
| `claim_is_exclusive` | **verified (4 steps)** | nothing — holds with no hypotheses |
| `capability_use_requires_claim` | **verified (2 steps)** | `cap_use_follows_grant` |
| `capability_is_bound_to_its_worker` | **verified (2 steps)** | `cap_use_follows_grant` |
| `completion_is_by_the_claimant` | **verified (2 steps)** | `cap_use_follows_grant` |
| `cap_use_follows_grant` | timeout (400 s) | — |
| `lease_was_granted` | timeout (1800 s) | — |
| 9 others, incl. `no_write_after_lease_loss`, `offer_authentication`, the `exec_*` | timeout (400 s) | — |

The "stands on" column is **measured, not assumed**: each lemma was re-run in a
copy of the theory with `[reuse]` stripped from `cap_use_follows_grant`. The
first two verify unchanged and are therefore unconditional. The other three
time out without it, so they are conditional on a lemma that is stated and
unproved.

The unproved keystone is `cap_use_follows_grant` — every use of a capability
was preceded by a claim by that worker at that version — and beneath it
`lease_was_granted`, which did not discharge at 1800 s. Both are about tracing
*where a lease came from* through rules that take it and put it back unchanged.
That chain is what remains open, here and in the reference model.

---

## The reference model — `models/resonate.spthy`

`-D=Core -D=Reduced`, 7 GB heap, 240 s per lemma.

| lemma | result |
|---|---|
| `grant_is_a_claim` | **verified (2 steps)** |
| `capability_use_requires_claim` | **verified (4 steps)** |
| `capability_is_bound_to_its_worker` | **verified (2 steps)** |
| `completion_is_by_the_claimant` | **verified (2 steps)** |
| everything else (22 lemmas) | timeout (240 s) |

Same status as the capability theory, and by the same measured dependency:
only `grant_is_a_claim` stands on its own; the other three rest on
`cap_use_follows_grant`, which did not discharge.

### Why it does not converge

The obstacle is branching, not depth, and it comes from a shape that runs
through the whole protocol: **rules that read state without changing it**.
`promise.get`, `task.get`, the retry dispatch, the heartbeat, `task.fence`, the
300 branch of `task.suspend` — each consumes a linear fact and puts back
exactly what it took, so each is a *source for its own premise*. Under
`-D=Core -D=Reduced` around twenty rules can produce a `PromiseSt`, so each
goal fans out twentyfold and each branch can re-enter the cycle.

---

## What moved the needle, and what did not

| attempt | effect |
|---|---|
| `sources_envelope` on the envelope key | **decisive** — without it precomputation never converges |
| `-D=Reduced`, `-D=Core` | **decisive** — 8.9 s to a first verdict, from never |
| splitting the heavy inductive lemma out of the `[reuse]` chain | **decisive** — 900 s timeout → 5 s |
| modelling versions as published nonces | **decisive for `claim_is_exclusive`** — 700 s timeout → 4 s |
| `--stop-on-trace=BFS` for exists-trace | **decisive where it applies** |
| proof-goal rankings (oracle, `--heuristic=I`, default) | none — six runs, six timeouts on the foundation lemmas |
| merging promise and task into one fact | none — see below |
| dropping payload echoes from responses | none measurable |
| collapsing identical repetitions | none measurable |

### `[reuse]` is not free

`grant_is_a_claim` — two steps, decidable by reading the rules — did not finish
in **900 s**. With the `reuse` lemmas removed from ahead of it: **4 s**.

| configuration | `grant_is_a_claim` | `capability_use_requires_claim` |
|---|---|---|
| `lease_was_granted [reuse, use_induction]` | timeout 900 s | timeout |
| `lease_was_granted [use_induction]` only | **verified, 5 s** | **verified, 5 s** |
| both helpers stripped of `[reuse]` | verified, 4 s | timeout |

A `reuse` lemma is saturated into every constraint system the prover builds,
for *every* later lemma — the `saturate under ∀-clauses (S_∀)` step the trace
grinds on. `lease_was_granted` quantifies over four variables and a timepoint,
and paying for it at every step costs more than it ever saves. But
`cap_use_follows_grant` genuinely needs `[reuse]`, so the two are split by
role: the heavy inductive statement is proved once and left alone, the light
consequence is carried forward.

### Versions as published nonces

`claim_is_exclusive` went from a 700 s timeout under three rankings to **4
steps, 4 seconds**. Every guard in the specification that reads a version tests
it by equality — fence, suspend, fulfill, release, heartbeat — and nothing
compares two versions for magnitude, so a guard cannot tell a nonce from a
counter. Ordering is lost and no lemma uses it; guessability is restored by
`Out`-ing every version as it is minted, which hands the adversary strictly
more than a real counter would, since forging a version the server never issued
matches no stored task.

`resonate-baseline.spthy` deliberately keeps the counter: there the version *is*
the authority to write, and its guessability is the point being made.

### Merging promise and task did not help

Tried, because the arithmetic said it should. 36 rules can produce a
`PromiseSt`, 25 can produce a `TaskSt`, and 21 rules need both in their
premises — so each of those faces 36 × 25 = 900 source combinations, against 37
for a single merged `Entry(S, pid, pstate, vblob, tstate, ver, holder)`.

The merged variant was built and measured: the fence rule went from four linear
premises to two, the callback drain from four to three. **All eight lemmas
timed out at 400 s on both the split and merged versions, to the second.**
Precomputation completes on both, so this measured proof search and not setup.
The arithmetic is real; it was not what dominated.

The merge is still worth doing for other reasons — it makes
`consistent_task_iff_targeted_promise` structural and collapses four rule pairs
that exist only because Tamarin cannot test the *absence* of a `TaskSt` — but
it should land against a model that is already proving.

### What would move the rest

`cap_use_follows_grant` and `lease_was_granted` are the whole of the remaining
gap. Both trace a lease backwards through rules that take it and return it
unchanged. Two things would plausibly close them, and neither is guesswork done
blind: an oracle written against the actual proof trees in `tamarin-prover
interactive`, which is how case studies of this size are normally driven; or
restating the state machine so reads do not consume — a generation counter on
each state fact, paid for elsewhere.

---

## How to reproduce

```sh
make check                                              # all four theories load
./prove.sh models/resonate-payload.spthy    600
./prove.sh models/resonate-capability.spthy 400
./prove.sh models/resonate-baseline.spthy   600
./prove.sh models/resonate.spthy            240 -D=Core -D=Reduced
```
