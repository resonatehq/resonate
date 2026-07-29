import Lake
open Lake DSL

package «resonate-spec» where
  -- pure Lean core; no external dependencies

@[default_target]
lean_lib «spec» where
  srcDir := "."
  roots  := #[]
  globs  := #[.submodules `«01-objects», .submodules `«02-actions-p»,
              .submodules `«02-actions-m», .submodules `«03-equivalence»]
