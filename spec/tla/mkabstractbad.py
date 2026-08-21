"""Generate AbstractBad: Abstract with creates removed from Next.

The anti-vacuity control. If the refinement still passes against THIS, the
check is measuring nothing -- a frozen mapping satisfies [][Next]_vars
trivially, because the UNCHANGED disjunct is always true. It must fail."""
s = open("Abstract.tla").read()
s = s.replace("MODULE Abstract", "MODULE AbstractBad", 1)
old = "    \\/ \\E req \\in CreateReq   : HandlePromiseCreate(req)"
assert old in s
s = s.replace(old, "    \\/ FALSE \\* CONTROL: creates removed. The refinement MUST now fail.", 1)
open("AbstractBad.tla", "w").write(s)
print("AbstractBad.tla generated from Abstract.tla")
