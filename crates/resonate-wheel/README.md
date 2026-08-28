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

## The specification, in four words

`spec_merge` is the whole definition, and it is meant to be read rather than
traced:

```
drop      every entry the batch mentions
add       the batch's updates -- one per timeout, the last one given
sort      by deadline, nearest first
cut       keep the first `capacity`
```

One sentence falls out of it: **after a merge the wheel holds the `capacity`
nearest deadlines of everything it was already keeping track of that the batch
did not touch, together with the batch itself.**

Note what the definition does *not* mention: scanning, indices, insertion
points, loops. `TimerWheel::merge` is proved *equal* to those four words, so the
implementation underneath is free to change — a heap, an index, a different scan
order — without the statement of correctness moving at all. That is the point of
writing it this way.

The one subtlety is in **add**. "Add the batch" would be wrong: a batch that
names one timeout twice would put it in the wheel twice, and the no-duplicates
invariant would be gone. So the second step adds the batch's *updates* — an
arrival is dropped only when a **later** arrival names the same timeout, which
is also what makes a batch read as a sequence of updates with the last one
standing. `spec_updates` is four lines and says exactly that.

### Three consequences worth knowing

**An update is not a lease on a slot.** The old entry is dropped in step 1, so
an arrival naming a timeout the wheel already holds inherits nothing — it
competes on its new deadline like anything else. Capacity 1, wheel holding
`A@1`, batch `[B@2, A@3]`: the union is `{A@3, B@2}`, `B` is nearer, so `B`
survives and `A` is gone. Moving a deadline outward can cost you the timeout.

**Within one batch, the last update wins.** See **add**, above.

**Ties sort behind, not ahead.** `spec_insert` places an entry after every entry
due no later than it, and the sort is that rule folded over the union — so it is
stable by construction. Untouched entries come before arrivals in the union, so
an entry the wheel was already holding keeps its place over an arrival that
merely ties with it.

## What is proved

`TimerWheel::merge` is proved *equal to* `spec_merge` and to re-establish
`wf`. On top of that sit three named theorems in `src/proof.rs`:

| theorem | statement |
| --- | --- |
| `lemma_merge_wf` | a merge always lands sorted, deduplicated and within capacity |
| `lemma_merge_horizon` | everything the cut dropped is due at or after everything it kept |
| `lemma_merge_ignores_far_future_newcomers` | merging *new* timeouts whose deadlines all sit beyond a full wheel's last entry changes nothing |
| `lemma_merge_preserves_no_duplicates` | no duplicates in, no duplicates out — whatever the batch does |
| `lemma_merge_replaces_or_evicts` | a timeout already held, updated by the batch, comes back with the new deadline or is pushed out — never survives stale |

The third is the headline: with capacity 1000, a batch whose deadlines are all
beyond the 1000th entry's is dropped whole. Its `fresh` hypothesis is
load-bearing and easy to overlook — an arrival that *shares* an identity with
an entry already in the wheel is not an addition but a move, so it takes that
entry's slot and the wheel does change, however far in the future it is. That
is intended (a deadline that moved out is still a deadline that moved), and it
is why the theorem is stated about newcomers.

`next` returns the nearest deadline without removing anything — the question a
scheduler asks between ticks. Its third postcondition is what makes the answer
usable: nothing in the wheel is due sooner. `pop_expired` carries the other end
of that guarantee, `now < next() ==> nothing comes back`, so a caller that
sleeps until the reported deadline cannot have missed a timer. (Despite the
name it does not advance anything and `TimerWheel` is not an `Iterator`;
repeated calls return the same deadline until the wheel changes.)

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

`lemma_merge_replaces_or_evicts` is the other half of that guarantee, and the
one that makes it load-bearing rather than trivial: a merge really does move
deadlines, and it moves them *by replacement*. If the wheel holds a timeout and
the batch carries an update for it, the merge leaves exactly two possibilities
— the timeout is back under the new deadline, or the wheel no longer holds that
identity at all, which is what happens when the new deadline lands beyond the
capacity horizon. The old entry is gone either way; there is no third outcome,
and no state in which the wheel holds it twice.

Its hypothesis is that no *later* arrival names the same identity — the last
update is the one that stands. Dropping it makes the theorem false, and Verus
says so.

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

`./verify.sh` -> **92 verified, 0 errors**, with no `assume`, no `admit`, and
no `external_body` anywhere in `src/`. `cargo test` -> 17 passed, plus the doctest.

The proofs were mutation-tested rather than taken on trust. Each line of the
four-word definition was broken in turn, and each break was caught:

| mutation | what Verus rejected |
| --- | --- |
| **drop** skipped, so old entries stay beside their updates | `merge`'s postcondition against `spec_merge` |
| **add** adds the whole batch instead of its last updates | the same — and this is the hole in the shorter phrasing |
| **add** keeps the *first* update per timeout instead of the last | the same |
| **sort** skipped | the same |
| **cut** skipped | the capacity bound in `wf` |
| `slot_for` stops *before* ties instead of after | its own postcondition, at the end of the scan |
| `no_duplicates` stated over all pairs, including `i == j` | unsatisfiable against reflexivity of `same` |
| the uniqueness hypothesis dropped from `lemma_merge_sets_identity` | the theorem is false without it |
| `next` returns the last deadline instead of the first | its minimum postcondition, and the tie in `pop_expired` |
| `pop_expired` cuts at a fixed index rather than by deadline | the split postconditions, and firing before `next` |

The second row is the one worth dwelling on. "Drop what the batch mentions, add
the batch, sort, cut" is the sentence everybody reaches for, and it is *almost*
right — it goes wrong only when a batch names one timeout twice, and then it
puts a duplicate in the wheel. That is why the spec says *the batch's updates*.

## Cost

`merge` is `O(m^2 + m(n + m))` for a wheel of `n` taking a batch of `m`: an
insertion sort of the batch, then one identity scan and one slot scan per
arrival. The wheel is sized for
the near horizon — thousands of entries, not millions — and a flat `Vec` beats
a heap or a hash index at that size on the operation that actually runs hot,
which is walking the front in deadline order. If a profile ever says otherwise, the
specification is the thing to keep — it says nothing about how the result is
computed, so a heap, a hash index or a merge sort can go in underneath without
the statement of correctness changing at all. That is exactly the freedom the
three-step definition is for.
