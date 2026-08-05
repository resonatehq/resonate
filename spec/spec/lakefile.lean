import Lake
open Lake DSL

package «resonate-spec» where
  -- pure Lean core; no external dependencies

/-- The specification: protocol surface and the two machines.
    `lake build spec` for the fast development loop. -/
@[default_target]
lean_lib «spec» where
  srcDir := "."
  roots  := #[]
  globs  := #[.submodules `«01-protocol», .submodules `«02-abstract»,
              .submodules `«03-concrete», .submodules `«05-sqlite»]

/-- The relations between machines — equivalence, refinement — with
    their exhaustive sweeps, which run under kernel `decide` at build
    time (minutes). `lake build` runs everything. -/
@[default_target]
lean_lib «theorems» where
  srcDir := "."
  roots  := #[]
  globs  := #[.submodules `«04-theorems»]

/-- The trace-admissibility shell. Imports `04-theorems`'s trace
    framework, modifies nothing in the specification. -/
@[default_target]
lean_lib «tracecheck» where
  srcDir := "."
  roots  := #[]
  globs  := #[.submodules `«06-trace»]

lean_exe «tracecheck_exe» where
  srcDir := "."
  root   := `«06-trace».experiments

lean_exe «realtrace_exe» where
  srcDir := "."
  root   := `«06-trace».real
