import Lake
open Lake DSL

package «resonate-spec» where
  -- pure Lean core; no external dependencies

/-! The package has two halves, and the directory layout says so.

`spec/` is the specification. `valid/` is a trace checker built ON it —
it imports `02-abstract` and modifies nothing. Siblings rather than
nesting the checker inside the numbered specification directories,
because one is the artifact and the other is a consumer. -/

/-- The specification: protocol surface and the machine.
    `lake build spec` for the fast development loop. -/
@[default_target]
lean_lib «spec» where
  srcDir := "spec"
  roots  := #[]
  globs  := #[.submodules `«01-protocol», .submodules `«02-abstract»]

/-- The system, the harnesses and the proofs. The sweeps here run under
    kernel `decide` at build time (minutes). `lake build` runs
    everything. -/
@[default_target]
lean_lib «theorems» where
  srcDir := "spec"
  roots  := #[]
  globs  := #[.submodules `«04-theorems»]

/-- The trace checker. -/
@[default_target]
lean_lib «valid» where
  srcDir := "."
  roots  := #[]
  globs  := #[.submodules `«valid»]

/-- `lake exe checktrace < trace.ndjson` — the one you want. -/
lean_exe «checktrace» where
  srcDir := "."
  root   := `«valid».«lean».check

/-- Scaling and cone-agreement measurements. -/
lean_exe «tracecheck_exe» where
  srcDir := "."
  root   := `«valid».«lean».experiments

/-- The hand-transcribed trace, plus tampered variants. -/
lean_exe «realtrace_exe» where
  srcDir := "."
  root   := `«valid».«lean».real

/-- The instants sweep: does firing a τ off the observation's instant ever
    change what an observer sees? -/
lean_exe «gap» where
  srcDir := "."
  root   := `«valid».«lean».gap
