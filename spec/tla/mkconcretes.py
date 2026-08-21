"""Derive ConcreteS from Concrete: the same executor, carrying a stuttering
variable so one atomic write can be walked one abstract step at a time.

ConcreteS used to be a hand-edited copy, which meant every change to Concrete
had to be made twice. It is generated now, so the two cannot drift."""
import re, sys

s = open("Concrete.tla").read()
def sub(old, new, n=1):
    global s
    assert s.count(old) >= 1, "pattern not found:\n" + old[:200]
    s = s.replace(old, new, n)

sub("MODULE Concrete", "MODULE ConcreteS")

# --- the variable ------------------------------------------------------------
sub("""VARIABLES
    docs,""", """(* `top` marks "not mid-walk". A record, so it cannot collide with a step. *)
top ==
    [top |-> "top"]

VARIABLES
    s,          \\* THE STUTTERING VARIABLE
    docs,""")
sub("vars ==\n    <<docs, timeouts, outbox, steps, now>>",
    "vars ==\n    <<docs, timeouts, outbox, steps, now>>\n\nvarsS ==\n    <<s, docs, timeouts, outbox, steps, now>>")

# --- the sweep records the abstract states it passes through ------------------
sub("""Advance(st, out) ==
    [ doc |-> PutsInto(st.doc, out.effects), fx |-> st.fx \\o out.effects ]""",
    """Advance(st, out) ==
    LET doc2 == PutsInto(st.doc, out.effects)
        fx2  == st.fx \\o out.effects
    IN
        [ doc |-> doc2, fx |-> fx2,
          tr  |-> IF out.effects = << >> THEN st.tr
                  ELSE st.tr \\o << [doc |-> doc2, fx |-> fx2] >> ]""")
sub("[doc |-> doc, fx |-> << >>]", "[doc |-> doc, fx |-> << >>, tr |-> << >>]")

# --- Process records the walk -------------------------------------------------
sub("""           W   == { [id |-> w.id, obj |-> final[w.id]] : w \\in Puts(fx) }
       IN  steps' = [steps EXCEPT ![r].phase   = "perform",""",
    """           W   == { [id |-> w.id, obj |-> final[w.id]] : w \\in Puts(fx) }
           walk == IF out.effects = << >> THEN swept.tr
                   ELSE swept.tr \\o << [doc |-> final, fx |-> fx] >>
       IN  steps' = [steps EXCEPT ![r].phase   = "perform",""")
sub("""                                  ![r].at      = now]
    /\\ UNCHANGED <<docs, timeouts, outbox, now>>""",
    """                                  ![r].at      = now,
                                  ![r].path    = IF walk = << >> THEN << >>
                                                 ELSE SubSeq(walk, 1, Len(walk) - 1)]
    /\\ UNCHANGED <<s, docs, timeouts, outbox, now>>""")
sub("""      expect |-> SetAsFun({}), at |-> 0, org |-> OriginOf(ev) ]""",
    """      expect |-> SetAsFun({}), at |-> 0, org |-> OriginOf(ev), path |-> << >> ]""")

# --- Perform: the write becomes a walk ---------------------------------------
sub("""    /\\ steps[r].phase = "perform"
    /\\ IF steps[r].pending = << >>
       THEN /\\ steps' = Drop(steps, r)
            /\\ UNCHANGED <<docs, timeouts, outbox>>""",
    """    /\\ steps[r].phase = "perform"
    (* A WALK BELONGS TO THE REQUEST THAT STARTED IT. Without this, another
       request's Perform steps someone else's path, or closes it with its own
       write. *)
    /\\ (s /= top) => (s.req = r)
    /\\ IF steps[r].pending = << >>
       THEN /\\ steps' = Drop(steps, r)
            /\\ UNCHANGED <<s, docs, timeouts, outbox>>""")
sub("""                    THEN /\\ docs'  = [docs EXCEPT ![o] =
                                         PutsInto(docs[o], steps[r].pending)]""",
    """                    THEN IF IF s = top THEN steps[r].path /= << >>
                                       ELSE s.rest /= << >>
                         THEN /\\ s' = IF s = top
                                      THEN [ req  |-> r,
                                             cur  |-> Head(steps[r].path),
                                             rest |-> Tail(steps[r].path),
                                             org  |-> o ]
                                      ELSE [ s EXCEPT !.cur  = Head(s.rest),
                                                      !.rest = Tail(s.rest) ]
                              /\\ UNCHANGED <<docs, timeouts, outbox, steps>>
                         ELSE
                         /\\ s' = top
                         /\\ docs'  = [docs EXCEPT ![o] =
                                         PutsInto(docs[o], steps[r].pending)]""")
sub("""                    ELSE /\\ steps' = [steps EXCEPT ![r].phase   = "process",
                                                   ![r].pending = << >>]
                         /\\ UNCHANGED <<docs, timeouts, outbox>>""",
    """                    ELSE /\\ s' = top
                         /\\ steps' = [steps EXCEPT ![r].phase   = "process",
                                                   ![r].pending = << >>]
                         /\\ UNCHANGED <<docs, timeouts, outbox>>""")
for tag in ("ArmTimeout", "DisarmTimeout"):
    sub('                 [] VariantTag(e) = "%s" ->\n                    /\\ timeouts\' =' % tag,
        '                 [] VariantTag(e) = "%s" ->\n                    /\\ UNCHANGED s\n                    /\\ timeouts\' =' % tag)
sub('                 [] OTHER ->\n                    /\\ outbox\' = LET en ==',
    '                 [] OTHER ->\n                    /\\ UNCHANGED s\n                    /\\ outbox\' = LET en ==')

# --- nothing else may interleave with a walk ---------------------------------
for act in ("SubmitExternal(ev) ==", "SubmitInternal(ev) ==", "Process(r) ==",
            "Crash(r) ==", "Clock =="):
    i = s.index("\n" + act); j = s.index("\n\n", i)
    lines = s[i:j].split("\n"); lines.insert(2, "    /\\ s = top")
    s = s[:i] + "\n".join(lines) + s[j:]
s = re.sub(r"UNCHANGED <<docs, timeouts, outbox, now>>",
           "UNCHANGED <<s, docs, timeouts, outbox, now>>", s)
sub("""    /\\ now' = now + 1
    /\\ UNCHANGED <<docs, timeouts, outbox, steps>>""",
    """    /\\ now' = now + 1
    /\\ UNCHANGED <<s, docs, timeouts, outbox, steps>>""")

sub("Init ==\n    /\\ docs", "Init ==\n    /\\ s        = top\n    /\\ docs")
s = re.sub(r"(Spec ==\n    Init /\\ \[\]\[Next\])_vars", r"\1_varsS", s)
s = re.sub(r"(SpecSF ==\n    Init /\\ \[\]\[Next\])_vars", r"\1_varsS", s)

open("ConcreteS.tla","w").write(s)
print("ConcreteS.tla derived from Concrete.tla")
