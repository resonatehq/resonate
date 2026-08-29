import «02-abstract».«system»
import «02-abstract».«properties»

/-!  # Legal — what must be true of a run

The system itself moved to `02-abstract/system.lean`: the alphabet, the
driver, `Trace`, and `Valid`. Those are the specification. What is left
here is the other half of the line — what a run has to SATISFY, which
folds the catalogue and therefore belongs with the theorems rather than
with the machine.

`valid_implies_legal` is the claim the conformance story rests on. Not
because it constrains an implementation — it mentions none — but because
it is what makes a catalogue violation UNAMBIGUOUS when an
implementation is checked. Without it a failing property leaves two live
hypotheses: the implementation is wrong, or the property is. With it,
the failure is a bug report, and the catalogue is portable.

Still `sorry`. What backs it today is two things at different strengths.
`holds.lean` proves 31 of the 94 entries outright — every `Valid` trace
from `init`, every schedule, every length, both read disciplines.
The remaining 63 are backed by `properties-check.lean` and
`properties-step.lean`: 1 464 scripts, both readings, `decide` at build
time, which `bounded.lean` shows is this same statement with both
quantifiers made finite.

Note the second hypothesis. `Valid` constrains steps, not starting
points: a trace beginning in an arbitrary state and stepping correctly
from there is `Valid` and can violate anything. Reachability is the
subject of the catalogue, and `(tr 0).state = init` is where it enters.

The provenance pair is not here and could not be — it is not in
`catalogue`. See `internalChecks`. -/

namespace Abstraction

/-! ## Legal

What it means for a run to satisfy the specification: every entry of the
catalogue (`02-abstract/properties.lean`), at every point of the trace,
applied to whatever its constructor says it takes.

`valid_implies_legal` is the claim the conformance story rests on. Not
because it constrains an implementation — it mentions none — but because
it is what makes a catalogue violation UNAMBIGUOUS when an
implementation is checked. Without it a failing property leaves two live
hypotheses: the implementation is wrong, or the property is. With it,
the failure is a bug report, and the catalogue is portable.

Stated with `sorry`. What backs it today is `properties-check.lean` and
`properties-step.lean` — 1 464 scripts, both readings, `decide` at build
time — which is a finite fact and not this.

Note the second hypothesis. `Valid` constrains steps, not starting
points: a trace beginning in an arbitrary state and stepping correctly
from there is `Valid` and can violate anything. Reachability is the
subject of the catalogue, and `(tr 0).state = init` is where it enters.

The provenance pair is not here and could not be — it is not in
`catalogue`. See `internalChecks`. -/

def Legal (tr : Trace) : Prop :=
  ∀ t : Nat, (AbstractModel.Properties.catalogue.all fun l =>
    match l.property with
    | .state f => f (tr t).now (tr t).state
    | .trans f => f (tr t).now (tr t).state (tr (t+1)).state) = true

theorem valid_implies_legal (mat : Bool) (tr : Trace)
    (hv : Valid mat tr) (h0 : (tr 0).state = AbstractModel.ServerState.init) :
    Legal tr := sorry


/-! ## Tag fixtures

The three tag shapes every corpus in this directory is built from. -/

/-! ## Tag fixtures

The three tag shapes every corpus in this directory is built from. -/

/-- Fixture ids.

    Every script in this directory lives in one origin. That is not
    decoration: the same-origin door refuses an awaits-edge that leaves
    its origin, so a script whose callback crossed origins would be
    answering 400 at the registration rather than exercising the path it
    was written for. Fixing the origin here lets each script name only
    the suffix, and makes the single-origin assumption visible in one
    place instead of implied in two hundred literals. -/
def oid (suffix : String) : ServerModel.Ident := { origin := "o", suffix }

def extTags   : ServerModel.Tags := [("resonate:external", "true")]
def tgtTags   : ServerModel.Tags := [("resonate:target", "w1")]
def timerTags : ServerModel.Tags := [("resonate:timer", "true")]

end Abstraction
