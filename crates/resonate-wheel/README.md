# resonate-wheel

A bounded, deduplicating timer wheel — specified, implemented, and proved to
agree, in one crate.

```
src/timeout.rs      Timeout<T>: a u64 deadline and a payload.
src/comparator.rs   Comparator<T>: which payloads are the same logical timeout.
                    Its equivalence laws are PROOF OBLIGATIONS on implementors,
                    not documentation. IdComparator covers the u64-keyed case.
src/spec.rs         THE SPECIFICATION, ghost throughout. `spec_merge` defines
                    what merging means in terms a reader can check by eye;
                    `sorted` and `distinct` are the wheel invariant.
src/proof.rs        THE PROOFS, in three layers -- sequence plumbing, then the
                    spec preserves the invariants, then the BRIDGE: the exec
                    code's single indexed Vec::remove/insert coincides with the
                    spec's filter and fold. Ends with the named corollaries.
src/wheel.rs        THE IMPLEMENTATION, over a flat Vec. Every method's
                    `ensures` names `spec_merge` and friends directly, so the
                    SPEC -- not the loops -- is the definition of the behaviour.
tests/wheel.rs      Worked examples, plus a differential test against an
                    independently written model.
```

## The data structure

A wheel holds `Timeout { deadline: u64, value: T }` under three rules, which
together are `TimerWheel::wf`:

- **Ordered.** Deadlines are non-decreasing, so the next timeout to fire is at
  index 0.
- **Deduplicated.** No two entries are the same logical timeout. What "same"
  means is the caller's: a wheel is built with a `Comparator`, and merging a
  timeout whose identity is already present *replaces* it. That is the
  operation a scheduler actually needs — a deadline moved, not a second entry
  for the same thing.
- **Bounded.** At most `capacity` entries. When a merge overflows, what falls
  off is the farthest future.

`deadline` is opaque: the wheel only ever compares deadlines, never does
arithmetic on them, so any monotone encoding works.

## Two decisions worth knowing about

**The batch is sorted, and capacity is enforced on every arrival.** These two
go together. Sorting nearest-deadline-first is what makes per-arrival capacity
well behaved: without it, a far-future arrival seen early could take the last
free slot and lock out a nearer one later in the same batch, so the result
would depend on the order the caller happened to supply. Sorted, capacity is
always spent on the nearest deadlines the batch contains.

Hoisting the cut out of the loop — enforcing capacity once at the end — looks
like a harmless optimisation and is not. The two rules agree except when an
arrival *replaces* an entry, which frees a slot mid-batch; see
`a_replacement_frees_a_slot_mid_batch` in the tests for the worked case. The
merge loop invariant rejects the edit.

**Ties sort behind, not ahead.** `spec_insert` places an arrival after every
entry due no later than it. So on a full wheel, an arrival that ties with the
last surviving deadline is the one dropped, and an entry already waiting keeps
its slot. The batch sort is `spec_insert` folded over the arrivals, so it
inherits the same rule and is stable for free.

One consequence to know before you rely on it: because the batch is applied
nearest-first, two arrivals in one batch sharing an identity resolve to the
**farthest** deadline, not the one listed last. If a batch is meant to carry a
sequence of updates to the same timeout, deduplicate it before handing it over.
`within_one_batch_the_farthest_deadline_wins` pins this down.

## What is proved

`TimerWheel::merge` is proved *equal to* `spec_merge` and to re-establish
`wf`. On top of that sit three named theorems in `src/proof.rs`:

| theorem | statement |
| --- | --- |
| `lemma_merge_wf` | a merge always lands sorted, deduplicated, within capacity — and never loses a slot the wheel was already using |
| `lemma_step_drops_the_farthest` | the entry an arrival displaces is the one due farthest in the future |
| `lemma_merge_ignores_far_future_newcomers` | merging *new* timeouts whose deadlines all sit beyond a full wheel's last entry changes nothing |
| `lemma_merge_preserves_no_duplicates` | no duplicates in, no duplicates out — whatever the batch does |

The third is the headline: with capacity 1000, a batch whose deadlines are all
beyond the 1000th entry's is dropped whole. Its `fresh` hypothesis is
load-bearing and easy to overlook — an arrival that *shares* an identity with
an entry already in the wheel is not an addition but a move, so it takes that
entry's slot and the wheel does change, however far in the future it is. That
is intended (a deadline that moved out is still a deadline that moved), and it
is why the theorem is stated about newcomers.

`pop_expired` is proved to split the wheel exactly: `r@ + self@ ==
old(self)@`, everything returned is due, nothing retained is. There is no
third bucket, so no timeout can be silently lost.

`lemma_merge_preserves_no_duplicates` is stated with a deliberately minimal
hypothesis: `no_duplicates(cmp, s)` and nothing else. No sortedness, no
capacity bound, and no condition at all on the incoming batch — it may repeat
an identity as often as it likes and collide with anything already held. So
deduplication does not depend on the wheel's other two invariants, and since
merging is the only way to put a timeout into a wheel, "the wheel never holds a
duplicate" is true of every wheel a caller can build. `merge` and `insert`
restate the conclusion in their own postconditions, so it is visible in the
signature without unfolding `wf`.

`no_duplicates` is the reader's form; `distinct` is the proof-friendly form
that quantifies over ordered pairs only. `lemma_no_duplicates_iff_distinct`
bridges them, and that bridge is exactly one use of the symmetry law. Note that
both are statements about distinct *positions*: `same` is reflexive, so a
version allowing `i == j` would be unsatisfiable for any non-empty wheel —
`cargo`-invisible, but Verus rejects it (mutation F below).

The comparator's three equivalence laws are what make dedup stable. Without
transitivity, "remove the entry we found" would not be the same as "remove
everything equivalent", and a replacement could leave behind a third entry
equivalent to the newcomer. `lemma_at_most_one_match` is where they are spent.

## One tree, two compilers

```
./verify.sh          # Verus: the spec, the proofs, and the tie to the code
cargo test           # the same sources under ghost erasure -- what ships
```

Both must pass. Under plain `cargo`, `verus_builtin_macros` erases every
`spec fn`, `proof fn`, `requires` and `ensures` syntactically, so the
specification costs nothing at runtime and an exec fn can `ensure` against
`spec_merge` while still compiling standing alone. The Verus toolchain is
**not** needed to build or use this crate — only to re-check the proofs.

One consequence of erasure, worth knowing before you edit: ghost items cannot
be named by a `use` outside a `verus!` block, because under cargo they do not
exist. Import their modules with a glob, as `src/proof.rs` does.

`verify.sh` looks for Verus at `$HOME/verus-build/verus/source/target-verus/release/verus`,
matching the convention in `resonatehq/resonate-verus`; override with `$VERUS`. Verus
pins its solver, so the build needs z3 4.16.0 on `PATH` (`pip install
z3-solver==4.16.0.0` is the shortest route).

Note the deliberate version skew. `verify.sh` checks the proofs against the
vstd that ships inside the Verus build; `Cargo.toml` pins the equivalent
crates.io releases for the erased build, so `cargo` never needs a Verus
toolchain. Keep the two in step when bumping either.

## Where things stand

`./verify.sh` -> **75 verified, 0 errors**, with no `assume`, no `admit`, and
no `external_body` anywhere in `src/`. `cargo test` -> 13 passed, plus the doctest.

The proofs were mutation-tested rather than taken on trust. Six semantic
changes were each made in isolation and each was caught:

| mutation | what Verus rejected |
| --- | --- |
| capacity hoisted out of the merge loop, cut once at the end | the merge loop invariant against `spec_merge_prefix` |
| the batch left unsorted | `merge`'s postcondition against `spec_merge` |
| `slot_for` stops *before* ties instead of after | its own postcondition, at the end of the scan |
| the dedup scan removed, so arrivals always append | `fresh` in `upsert_uncapped` -- the wheel could hold a duplicate |
| the removal step dropped from the no-duplicate proof | `lemma_step_preserves_distinct` -- freshness no longer follows |
| `no_duplicates` stated over all pairs, including `i == j` | unsatisfiable against reflexivity of `same` |

The first two are the ones to keep in mind when editing: both read as tidying
and both change the result.

## Cost

`merge` is `O(m^2 + m(n + m))` for a wheel of `n` taking a batch of `m`: an
insertion sort of the batch, then one identity scan and one slot scan per
arrival. The wheel is sized for
the near horizon — thousands of entries, not millions — and a flat `Vec` beats
a heap or a hash index at that size on the operation that actually runs hot,
which is walking the front in deadline order. If a profile ever says otherwise,
the specification is the thing to keep: `spec_merge` says nothing about how the
result is computed, so an index can be added underneath it without the
statement of correctness changing at all.
