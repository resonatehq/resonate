"""Derive ConcreteS from Concrete: the same executor, carrying a stuttering
variable so one atomic write can be walked one abstract step at a time.

The variable holds a POSITION, not a path. The sequence of stores a write
passes through is a function of the document it decided against, the instant
it decided, and the request -- all of which `steps` already holds, and none of
which move while a walk is in progress. So it is recomputed, not stored: `s`
is a request id and a counter.

ConcreteS used to be a hand-edited copy, which meant every change to Concrete
had to be made twice."""
import re

s = open("Concrete.tla").read()
def sub(old, new):
    global s
    assert s.count(old) >= 1, "pattern not found:\n" + old[:200]
    s = s.replace(old, new, 1)

sub("MODULE Concrete", "MODULE ConcreteS")

sub("""VARIABLES
    docs,""", """(* `top` marks "not mid-walk". A record, so it cannot collide with a
   position. *)
top ==
    [top |-> "top"]

VARIABLES
    s,          \\* THE STUTTERING VARIABLE: [req |-> Rid, k |-> Nat], or top
    docs,""")
sub("vars ==\n    <<docs, timeouts, outbox, steps, now>>",
    "vars ==\n    <<docs, timeouts, outbox, steps, now>>\n\nvarsS ==\n    <<s, docs, timeouts, outbox, steps, now>>")

# --- the sweep records the stores it passes through (a value, not a variable) --
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

sub("""(* Read ONE DOCUMENT, decide against it""",
    """(* THE STORES ONE WRITE PASSES THROUGH, one per abstract step, excluding the
   last -- the last is what the write itself produces. Recomputed on demand at
   every tick, which costs evaluation and saves state. *)
Walk(doc, ev, t) ==
    LET swept == Sweep(doc, t)
        out   == Handle(ev, [ objects  |-> swept.doc,
                              timeouts |-> timeouts,
                              outbox   |-> outbox,
                              now      |-> t,
                              config   |-> [retryTimeout |-> RetryTimeout] ])
        full  == IF out.effects = << >> THEN
                     swept.tr
                 ELSE
                     swept.tr \\o << [ doc |-> PutsInto(doc, swept.fx \\o out.effects),
                                      fx  |-> swept.fx \\o out.effects ] >>
    IN
        IF full = << >> THEN << >> ELSE SubSeq(full, 1, Len(full) - 1)

(* Read ONE DOCUMENT, decide against it""")

sub("""                                  ![r].at      = now]
    /\\ UNCHANGED <<docs, timeouts, outbox, now>>""",
    """                                  ![r].at      = now]
    /\\ UNCHANGED <<s, docs, timeouts, outbox, now>>""")

# --- Perform: the write becomes a walk ---------------------------------------
sub("""    /\\ steps[r].phase = "perform"
    /\\ IF steps[r].pending = << >>
       THEN /\\ steps' = Drop(steps, r)
            /\\ UNCHANGED <<docs, timeouts, outbox>>""",
    """    /\\ steps[r].phase = "perform"
    (* A WALK BELONGS TO THE REQUEST THAT STARTED IT. Without this, another
       request's Perform steps someone else's walk, or closes it with its own
       write. *)
    /\\ (s /= top) => (s.req = r)
    /\\ IF steps[r].pending = << >>
       THEN /\\ steps' = Drop(steps, r)
            /\\ UNCHANGED <<s, docs, timeouts, outbox>>""")
sub("""                    THEN /\\ docs'  = [docs EXCEPT ![o] =
                                         PutsInto(docs[o], steps[r].pending)]""",
    """                    THEN IF IF s = top
                            THEN Walk(docs[o], steps[r].ev, now) /= << >>
                            ELSE s.k < Len(Walk(docs[o], steps[r].ev, now))
                         THEN /\\ s' = IF s = top THEN [req |-> r, k |-> 1]
                                      ELSE [s EXCEPT !.k = @ + 1]
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
