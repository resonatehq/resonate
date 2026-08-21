"""Generate the anti-vacuity control: Abstract with creates removed, and a
Refinement pointed at it.

If the refinement still passes against THIS, the check is measuring nothing --
a frozen mapping satisfies [][Next]_vars trivially, because the UNCHANGED
disjunct is always true. It must fail, and it must fail fast."""
a = open("Abstract.tla").read().replace("MODULE Abstract", "MODULE AbstractBad", 1)
old = "    \\/ \\E req \\in CreateReq   : HandlePromiseCreate(req)"
assert old in a
open("AbstractBad.tla","w").write(
    a.replace(old, "    \\/ FALSE \\* CONTROL: creates removed. The refinement MUST now fail.", 1))

r = open("Refinement.tla").read()
r = r.replace("MODULE Refinement", "MODULE RefinementBad", 1)
r = r.replace("INSTANCE Abstract WITH", "INSTANCE AbstractBad WITH", 1)
r = r.replace("Refines ==", "RefinesBad ==", 1)
i = r.index("(* THE AUXILIARY VARIABLE"); j = r.index("====", i)
open("RefinementBad.tla","w").write(r[:i] + r[j:])

c = open("Refinement.cfg").read()
open("RefinementBad.cfg","w").write(c[:c.index("PROPERTY")] + "PROPERTY\n    RefinesBad\n")
print("AbstractBad.tla, RefinementBad.tla generated")
