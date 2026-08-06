/-!  # A SQL engine, in Lean

The effect is `query : String → List SqlValue → m QueryResult`. Nothing
else. A handler holds the statement TEXT — the same characters that reach
`rusqlite` — and this file tokenizes, parses and evaluates it against a
generic relational store that has never heard of a promise.

That is the whole point, and it is worth stating why it is not the same
exercise as a typed effect interface. A `selectPromise : String → m
(Option PromiseRow)` method with the SQL in its docstring compares an
implementation against the author's PARAPHRASE of the SQL; the docstring
is decoration, and a mistake in the paraphrase is invisible because the
paraphrase is what runs. Here the statement is the artifact under test,
so a divergence between the specification and the SQL is a divergence the
sweep can see.

Everything below is total and pure. Recursion is fuelled rather than
well-founded, so kernel reduction never has to discharge a termination
argument mid-`decide`, and the string scans are structural — the
specification's own constraint, so that no proof needs `native_decide`.

## The subset

Exactly what the `promise.get` path issues, and no more:

* `SELECT [DISTINCT] cols FROM t [alias] [JOIN u alias ON e] [WHERE e]`
* `UPDATE t SET c = e, … [WHERE e]`
* `DELETE FROM t [WHERE e]`
* `INSERT INTO t (cols) VALUES (es) [ON CONFLICT (c) DO UPDATE SET …]`
* `INSERT INTO t (cols) SELECT … [ON CONFLICT DO NOTHING]`
* expressions: columns (optionally qualified), `?n`, string / integer /
  `NULL` / `true` / `false` literals, `= != < <= > >=`, `AND`, `OR`,
  `IN (subselect)`, `EXCLUDED.c`
* the `json_each(?n)` table-valued function, over a JSON array of strings

An unparseable or unsupported statement evaluates to `.error`, which the
conformance sweep treats as a failure rather than a silent no-op. -/

namespace Sql

/-! ## Values

SQLite has no boolean type: `true` is `1`. Modelling that rather than
adding a `.bool` constructor keeps `ready = true` and `ready = 1`
indistinguishable, which is what the engine does. -/

inductive SqlValue
  | null
  | int  (n : Int)
  | text (s : String)
  deriving Repr, DecidableEq, Inhabited

/-- A row keyed by column name. A source with an alias contributes its
    columns twice — bare and `alias.`-prefixed — so `c.awaiter_id` and
    `awaiter_id` are both lookups rather than a scoping discipline. -/
abbrev Row   := List (String × SqlValue)
abbrev Table := List Row
/-- The store. No schema: a table is a name and a list of rows, and the
    engine never learns what a promise is. -/
abbrev Store := List (String × Table)

def Row.get? (r : Row) (c : String) : Option SqlValue :=
  (r.find? (·.fst == c)).map (·.snd)

def Store.table (s : Store) (n : String) : Table :=
  ((s.find? (·.fst == n)).map (·.snd)).getD []

def Store.setTable (s : Store) (n : String) (t : Table) : Store :=
  if s.any (·.fst == n) then
    s.map fun e => if e.fst == n then (n, t) else e
  else
    s ++ [(n, t)]

structure QueryResult where
  rows    : List Row := []
  changes : Nat      := 0
  error   : Bool     := false
  deriving Repr, DecidableEq

/-! ## Tokens -/

inductive Tok
  | ident (s : String)
  | str   (s : String)
  | num   (n : Int)
  | param (n : Nat)
  | punct (s : String)
  deriving Repr, DecidableEq

private def isSpace (c : Char) : Bool :=
  c == ' ' || c == '\n' || c == '\t' || c == '\r'

private def isDigit (c : Char) : Bool := '0' ≤ c && c ≤ '9'

private def isIdent (c : Char) : Bool :=
  ('a' ≤ c && c ≤ 'z') || ('A' ≤ c && c ≤ 'Z') || isDigit c || c == '_'

private def toUpper (s : String) : String :=
  (s.toList.map fun c => if 'a' ≤ c && c ≤ 'z' then
      Char.ofNat (c.toNat - 32) else c).asString

private def digits : List Char → Nat → Nat × List Char
  | c :: cs, acc => if isDigit c then digits cs (acc * 10 + (c.toNat - 48)) else (acc, c :: cs)
  | [],      acc => (acc, [])

private def identChars : List Char → List Char → String × List Char
  | c :: cs, acc => if isIdent c then identChars cs (acc ++ [c]) else (acc.asString, c :: cs)
  | [],      acc => (acc.asString, [])

private def strChars : List Char → List Char → String × List Char
  | '\'' :: cs, acc => (acc.asString, cs)
  | c    :: cs, acc => strChars cs (acc ++ [c])
  | [],         acc => (acc.asString, [])

def tokenize : Nat → List Char → List Tok
  | 0,      _       => []
  | _,      []      => []
  | fuel+1, c :: cs =>
    if isSpace c then tokenize fuel cs
    else if c == '\'' then
      let (s, rest) := strChars cs []
      .str s :: tokenize fuel rest
    else if c == '?' then
      let (n, rest) := digits cs 0
      .param n :: tokenize fuel rest
    else if isDigit c then
      let (n, rest) := digits (c :: cs) 0
      .num (Int.ofNat n) :: tokenize fuel rest
    else if isIdent c then
      let (s, rest) := identChars (c :: cs) []
      .ident s :: tokenize fuel rest
    else
      match c, cs with
      | '!', '=' :: rest => .punct "!=" :: tokenize fuel rest
      | '<', '=' :: rest => .punct "<=" :: tokenize fuel rest
      | '>', '=' :: rest => .punct ">=" :: tokenize fuel rest
      | _,   rest        => .punct ([c].asString) :: tokenize fuel rest

/-- Every token consumes at least one character, so the length is fuel
    enough. Fuelled rather than well-founded so that kernel reduction
    never meets a termination obligation mid-`decide`. -/
def tokenizeAll (sql : String) : List Tok :=
  tokenize (sql.length + 1) sql.toList

/-! ## Syntax -/

mutual

inductive Expr
  | col      (qual : Option String) (name : String)
  | excluded (name : String)
  | param    (n : Nat)
  | lit      (v : SqlValue)
  | bin      (op : String) (l r : Expr)
  | inSel    (e : Expr) (q : Select)
  deriving Repr

inductive Source
  | table    (name : String) (alias : Option String)
  | jsonEach (e : Expr)
  deriving Repr

inductive Select
  | mk (distinct : Bool) (cols : List Expr) (star : Bool)
       (src : Source) (joins : List (Source × Expr)) (wh : Option Expr)
  deriving Repr

end

inductive Conflict
  | none
  | doNothing
  | doUpdate (key : String) (sets : List (String × Expr))
  deriving Repr

inductive Stmt
  | select (q : Select)
  | update (table : String) (sets : List (String × Expr)) (wh : Option Expr)
  | delete (table : String) (wh : Option Expr)
  | insertValues (table : String) (cols : List String) (vals : List Expr) (onC : Conflict)
  | insertSelect (table : String) (cols : List String) (q : Select) (onC : Conflict)
  deriving Repr

/-! ## Parser

Recursive descent over the token list, fuelled. Every production returns
the tokens it did not consume, and `none` on a shape outside the subset —
the engine reports that as an error rather than guessing. -/

abbrev P (α) := List Tok → Option (α × List Tok)

private def kw (k : String) : P Unit
  | .ident s :: ts => if toUpper s == k then some ((), ts) else none
  | _ => none

private def kwOpt (k : String) (ts : List Tok) : Bool × List Tok :=
  match kw k ts with | some (_, r) => (true, r) | none => (false, ts)

private def pun (p : String) : P Unit
  | .punct s :: ts => if s == p then some ((), ts) else none
  | _ => none

private def name : P String
  | .ident s :: ts => some (s, ts)
  | _ => none

private def RESERVED : List String :=
  ["FROM","WHERE","JOIN","ON","SET","VALUES","SELECT","INSERT","UPDATE",
   "DELETE","INTO","CONFLICT","DO","NOTHING","DISTINCT","AND","OR","IN","IS","NOT",
   "NULL","TRUE","FALSE","EXCLUDED"]

/-- An identifier that is not a keyword — used for table aliases, where
    `FROM listeners l WHERE …` must not read `WHERE` as the alias. -/
private def alias? : P (Option String)
  | .ident s :: ts => if RESERVED.contains (toUpper s) then some (none, .ident s :: ts)
                      else some (some s, ts)
  | ts => some (none, ts)

mutual

private def pExpr : Nat → P Expr
  | 0,     _  => none
  | fuel+1, ts => do
    let (l, ts) ← pCmp fuel ts
    pExprTail fuel l ts

private def pExprTail : Nat → Expr → P Expr
  | 0,      _, _  => none
  | fuel+1, l, ts =>
    match kw "AND" ts with
    | some (_, ts) => do let (r, ts) ← pCmp fuel ts; pExprTail fuel (.bin "AND" l r) ts
    | none =>
      match kw "OR" ts with
      | some (_, ts) => do let (r, ts) ← pCmp fuel ts; pExprTail fuel (.bin "OR" l r) ts
      | none => some (l, ts)

/-- Additive level, between comparison and atom: `version + 1`. -/
private def pAdd : Nat → P Expr
  | 0,      _  => none
  | fuel+1, ts => do
    let (l, ts) ← pAtom fuel ts
    pAddTail fuel l ts

private def pAddTail : Nat → Expr → P Expr
  | 0,      _, _  => none
  | fuel+1, l, ts =>
    match ts with
    | .punct o :: ts' =>
        if o == "+" || o == "-" then do
          let (r, ts'') ← pAtom fuel ts'
          pAddTail fuel (.bin o l r) ts''
        else some (l, ts)
    | _ => some (l, ts)

private def pCmp : Nat → P Expr
  | 0,      _  => none
  | fuel+1, ts => do
    let (l, ts) ← pAdd fuel ts
    match ts with
    | .punct o :: ts' =>
        if o == "=" || o == "!=" || o == "<" || o == "<=" || o == ">" || o == ">=" then do
          let (r, ts'') ← pAdd fuel ts'
          some (.bin o l r, ts'')
        else some (l, ts)
    | _ =>
      match kw "IS" ts with
      | some (_, ts') =>
          -- `IS NULL` / `IS NOT NULL`: the only null test SQL has, since
          -- `= NULL` is NULL rather than true.
          (match kw "NOT" ts' with
           | some (_, ts'') => do
               let (_, ts'') ← kw "NULL" ts''
               some (.bin "ISNOT" l (.lit .null), ts'')
           | none => do
               let (_, ts'') ← kw "NULL" ts'
               some (.bin "IS" l (.lit .null), ts''))
      | none =>
      match kw "IN" ts with
      | some (_, ts') => do
          let (_, ts') ← pun "(" ts'
          let (q, ts') ← pSelect fuel ts'
          let (_, ts') ← pun ")" ts'
          some (.inSel l q, ts')
      | none => some (l, ts)

private def pAtom : Nat → P Expr
  | 0,      _  => none
  | fuel+1, ts =>
    match ts with
    | .param n :: ts => some (.param n, ts)
    | .str s   :: ts => some (.lit (.text s), ts)
    | .num n   :: ts => some (.lit (.int n), ts)
    | .ident s :: ts =>
        let u := toUpper s
        if u == "NULL"  then some (.lit .null, ts)
        else if u == "TRUE"  then some (.lit (.int 1), ts)
        else if u == "FALSE" then some (.lit (.int 0), ts)
        else
          match ts with
          | .punct "." :: .ident c :: ts' =>
              if u == "EXCLUDED" then some (.excluded c, ts')
              else some (.col (some s) c, ts')
          | _ => some (.col none s, ts)
    | .punct "(" :: ts => do
        let (e, ts) ← pExpr fuel ts
        let (_, ts) ← pun ")" ts
        some (e, ts)
    | _ => none

private def pSource : Nat → P Source
  | 0,      _  => none
  | fuel+1, ts =>
    match ts with
    | .ident f :: .punct "(" :: ts' =>
        if toUpper f == "JSON_EACH" then do
          let (e, ts') ← pExpr fuel ts'
          let (_, ts') ← pun ")" ts'
          some (.jsonEach e, ts')
        else none
    | _ => do
        let (t, ts) ← name ts
        let (a, ts) ← alias? ts
        some (.table t a, ts)

private def pJoins : Nat → List (Source × Expr) → P (List (Source × Expr))
  | 0,      _,   _  => none
  | fuel+1, acc, ts =>
    match kw "JOIN" ts with
    | none => some (acc, ts)
    | some (_, ts) => do
        let (s, ts) ← pSource fuel ts
        let (_, ts) ← kw "ON" ts
        let (e, ts) ← pExpr fuel ts
        pJoins fuel (acc ++ [(s, e)]) ts

private def pCols : Nat → List Expr → P (List Expr)
  | 0,      _,   _  => none
  | fuel+1, acc, ts => do
    let (e, ts) ← pAtom fuel ts
    match pun "," ts with
    | some (_, ts) => pCols fuel (acc ++ [e]) ts
    | none         => some (acc ++ [e], ts)

private def pSelect : Nat → P Select
  | 0,      _  => none
  | fuel+1, ts => do
    let (_, ts) ← kw "SELECT" ts
    let (dist, ts) := kwOpt "DISTINCT" ts
    match pun "*" ts with
    | some (_, ts) => do
        let (_, ts) ← kw "FROM" ts
        let (src, ts) ← pSource fuel ts
        let (js, ts) ← pJoins fuel [] ts
        match kw "WHERE" ts with
        | some (_, ts) => do let (w, ts) ← pExpr fuel ts; some (.mk dist [] true src js (some w), ts)
        | none => some (.mk dist [] true src js none, ts)
    | none => do
        let (cols, ts) ← pCols fuel [] ts
        let (_, ts) ← kw "FROM" ts
        let (src, ts) ← pSource fuel ts
        let (js, ts) ← pJoins fuel [] ts
        match kw "WHERE" ts with
        | some (_, ts) => do let (w, ts) ← pExpr fuel ts; some (.mk dist cols false src js (some w), ts)
        | none => some (.mk dist cols false src js none, ts)

end

private def pSets : Nat → List (String × Expr) → P (List (String × Expr))
  | 0,      _,   _  => none
  | fuel+1, acc, ts => do
    let (c, ts) ← name ts
    let (_, ts) ← pun "=" ts
    let (e, ts) ← pExpr 64 ts
    match pun "," ts with
    | some (_, ts) => pSets fuel (acc ++ [(c, e)]) ts
    | none         => some (acc ++ [(c, e)], ts)

private def pNames : Nat → List String → P (List String)
  | 0,      _,   _  => none
  | fuel+1, acc, ts => do
    let (c, ts) ← name ts
    match pun "," ts with
    | some (_, ts) => pNames fuel (acc ++ [c]) ts
    | none         => some (acc ++ [c], ts)

private def pExprList : Nat → List Expr → P (List Expr)
  | 0,      _,   _  => none
  | fuel+1, acc, ts => do
    let (e, ts) ← pExpr 64 ts
    match pun "," ts with
    | some (_, ts) => pExprList fuel (acc ++ [e]) ts
    | none         => some (acc ++ [e], ts)

private def pConflict : P Conflict := fun ts =>
  match kw "ON" ts with
  | none => some (.none, ts)
  | some (_, ts) => do
    let (_, ts) ← kw "CONFLICT" ts
    match pun "(" ts with
    | some (_, ts) => do
        let (k, ts) ← name ts
        let (_, ts) ← pun ")" ts
        let (_, ts) ← kw "DO" ts
        match kw "NOTHING" ts with
        | some (_, ts) => some (.doNothing, ts)
        | none => do
            let (_, ts) ← kw "UPDATE" ts
            let (_, ts) ← kw "SET" ts
            let (ss, ts) ← pSets 32 [] ts
            some (.doUpdate k ss, ts)
    | none => do
        let (_, ts) ← kw "DO" ts
        let (_, ts) ← kw "NOTHING" ts
        some (.doNothing, ts)

private def pStmt : P Stmt := fun ts =>
  match kw "SELECT" ts with
  | some _ => do let (q, ts) ← pSelect 64 ts; some (.select q, ts)
  | none =>
  match kw "UPDATE" ts with
  | some (_, ts) => do
      let (t, ts) ← name ts
      let (_, ts) ← kw "SET" ts
      let (ss, ts) ← pSets 32 [] ts
      match kw "WHERE" ts with
      | some (_, ts) => do let (w, ts) ← pExpr 64 ts; some (.update t ss (some w), ts)
      | none => some (.update t ss none, ts)
  | none =>
  match kw "DELETE" ts with
  | some (_, ts) => do
      let (_, ts) ← kw "FROM" ts
      let (t, ts) ← name ts
      match kw "WHERE" ts with
      | some (_, ts) => do let (w, ts) ← pExpr 64 ts; some (.delete t (some w), ts)
      | none => some (.delete t none, ts)
  | none =>
  match kw "INSERT" ts with
  | some (_, ts) => do
      let (_, ts) ← kw "INTO" ts
      let (t, ts) ← name ts
      let (_, ts) ← pun "(" ts
      let (cols, ts) ← pNames 32 [] ts
      let (_, ts) ← pun ")" ts
      match kw "VALUES" ts with
      | some (_, ts) => do
          let (_, ts) ← pun "(" ts
          let (vs, ts) ← pExprList 32 [] ts
          let (_, ts) ← pun ")" ts
          let (c, ts) ← pConflict ts
          some (.insertValues t cols vs c, ts)
      | none => do
          let (q, ts) ← pSelect 64 ts
          let (c, ts) ← pConflict ts
          some (.insertSelect t cols q c, ts)
  | none => none

def parse (sql : String) : Option Stmt :=
  match pStmt (tokenizeAll sql) with
  | some (s, []) => some s
  | _            => none

/-! ## Evaluation -/

/-- `json_each` over a JSON array of strings — the shape
    `serde_json::to_string(&[&str])` produces. Not a JSON parser; it
    handles arrays of quoted strings and nothing else, which is the only
    thing the driver passes it. -/
def jsonStringsOf (s : String) : List String :=
  let rec go : List Char → List Char → Bool → List String → List String
    | [],          cur, inS, acc => if inS then acc ++ [cur.asString] else acc
    | '"'  :: cs,  cur, inS, acc => if inS then go cs [] false (acc ++ [cur.asString]) else go cs [] true acc
    | c    :: cs,  cur, inS, acc => if inS then go cs (cur ++ [c]) true acc else go cs cur false acc
  go s.toList [] false []

private def qualify (a : Option String) (r : Row) : Row :=
  match a with
  | none    => r
  | some al => r ++ r.map fun (c, v) => (al ++ "." ++ c, v)

private def truthy : SqlValue → Bool
  | .int n => n != 0
  | .text s => s != ""
  | .null   => false

mutual

private def evalExpr : Nat → Store → List SqlValue → Row → Row → Expr → SqlValue
  | 0,      _,  _,  _, _,  _ => .null
  | fuel+1, st, ps, r, ex, e =>
    match e with
    | .col q c =>
        let key := match q with | none => c | some qq => qq ++ "." ++ c
        (r.get? key).getD .null
    | .excluded c => (ex.get? c).getD .null
    | .param n    => (ps[n - 1]?).getD .null
    | .lit v      => v
    | .bin op l rr =>
        let a := evalExpr fuel st ps r ex l
        let b := evalExpr fuel st ps r ex rr
        match op with
        | "AND" => if truthy a && truthy b then .int 1 else .int 0
        | "OR"  => if truthy a || truthy b then .int 1 else .int 0
        -- `IS NULL` is total: it is the one test that does not propagate
        | "IS"    => if a == .null then .int 1 else .int 0
        | "ISNOT" => if a == .null then .int 0 else .int 1
        | "+" => (match a, b with | .int x, .int y => .int (x + y) | _, _ => .null)
        | "-" => (match a, b with | .int x, .int y => .int (x - y) | _, _ => .null)
        -- SQLite three-valued logic: any comparison with NULL is NULL,
        -- and NULL is not truthy, so a WHERE on it excludes the row.
        | _ =>
          match a, b with
          | .null, _ => .null
          | _, .null => .null
          | _, _ =>
            let c : Bool :=
              match op, a, b with
              | "=",  x, y => x == y
              | "!=", x, y => x != y
              | "<",  .int x, .int y => x < y
              | "<=", .int x, .int y => x ≤ y
              | ">",  .int x, .int y => x > y
              | ">=", .int x, .int y => x ≥ y
              | _, _, _ => false
            if c then .int 1 else .int 0
    | .inSel l q =>
        let v := evalExpr fuel st ps r ex l
        let rows := evalSelect fuel st ps q
        if rows.any (fun rr => rr.any (fun (_, w) => w == v)) then .int 1 else .int 0

private def evalSource : Nat → Store → List SqlValue → Source → Table
  | 0,      _,  _,  _ => []
  | fuel+1, st, ps, s =>
    match s with
    | .table n a  => (st.table n).map (qualify a)
    | .jsonEach e =>
        match evalExpr fuel st ps [] [] e with
        | .text j => (jsonStringsOf j).map fun v => [("value", .text v)]
        | _       => []

private def evalSelect : Nat → Store → List SqlValue → Select → List Row
  | 0,      _,  _,  _ => []
  | fuel+1, st, ps, .mk dist cols star src joins wh =>
    let base := evalSource fuel st ps src
    let joined := joins.foldl (init := base) fun acc (s, on) =>
      let right := evalSource fuel st ps s
      acc.flatMap fun l =>
        right.filterMap fun rr =>
          let merged := l ++ rr
          if truthy (evalExpr fuel st ps merged [] on) then some merged else none
    let filtered := match wh with
      | none   => joined
      | some w => joined.filter fun r => truthy (evalExpr fuel st ps r [] w)
    let projected :=
      if star then filtered
      else filtered.map fun r =>
        cols.map fun c =>
          let nm := match c with
            | .col _ n => n
            | _        => "?"
          (nm, evalExpr fuel st ps r [] c)
    if dist then
      projected.foldl (init := []) fun acc r => if acc.contains r then acc else acc ++ [r]
    else projected

end

private def applySets (fuel : Nat) (st : Store) (ps : List SqlValue)
    (r ex : Row) (sets : List (String × Expr)) : Row :=
  sets.foldl (init := r) fun acc (c, e) =>
    let v := evalExpr fuel st ps r ex e
    if acc.any (·.fst == c) then acc.map (fun p => if p.fst == c then (c, v) else p)
    else acc ++ [(c, v)]

/-- Run a statement. Returns the rows it produced, how many it changed —
    the `execute() > 0` the driver branches on — and whether the text was
    outside the subset. -/
def exec (st : Store) (sql : String) (ps : List SqlValue) : QueryResult × Store :=
  match parse sql with
  | none => ({ error := true }, st)
  | some stmt =>
    let fuel := 64
    match stmt with
    | .select q => ({ rows := evalSelect fuel st ps q }, st)
    | .delete t wh =>
        let rows := st.table t
        let keep := rows.filter fun r =>
          match wh with | none => false | some w => !truthy (evalExpr fuel st ps r [] w)
        ({ changes := rows.length - keep.length }, st.setTable t keep)
    | .update t sets wh =>
        let rows := st.table t
        let hit := fun r => match wh with
          | none   => true
          | some w => truthy (evalExpr fuel st ps r [] w)
        let n := (rows.filter hit).length
        let rows' := rows.map fun r => if hit r then applySets fuel st ps r [] sets else r
        ({ changes := n }, st.setTable t rows')
    | .insertValues t cols vals onC =>
        let row : Row := (cols.zip vals).map fun (c, e) => (c, evalExpr fuel st ps [] [] e)
        insertRows st t [row] onC fuel ps
    | .insertSelect t cols q onC =>
        let produced := evalSelect fuel st ps q
        let rows : List Row := produced.map fun r =>
          (cols.zip (r.map (·.snd))).map fun (c, v) => (c, v)
        insertRows st t rows onC fuel ps
where
  /-- `ON CONFLICT` on a single-column key, plus the whole-row identity
      `DO NOTHING` uses when it names no key. -/
  insertRows (st : Store) (t : String) (rows : List Row) (onC : Conflict)
      (fuel : Nat) (ps : List SqlValue) : QueryResult × Store :=
    rows.foldl (init := (({} : QueryResult), st)) fun (acc, st) row =>
      let existing := st.table t
      let clash := match onC with
        | .doUpdate k _ => existing.find? fun r => Row.get? r k == Row.get? row k
        | _             => existing.find? fun r => r == row
      match clash, onC with
      | some _, .doNothing => (acc, st)
      | some old, .doUpdate k sets =>
          let upd := applySets fuel st ps old row sets
          (⟨acc.rows, acc.changes + 1, acc.error⟩,
           st.setTable t (existing.map fun r => if Row.get? r k == Row.get? old k then upd else r))
      | some _, .none => (acc, st)
      | none, _ => (⟨acc.rows, acc.changes + 1, acc.error⟩, st.setTable t (existing ++ [row]))

/-! ## The effect

One method. A handler that wants the store must go through a statement,
and the statement is text. -/

class MonadQuery (m : Type → Type) where
  query : String → List SqlValue → m QueryResult

export MonadQuery (query)

abbrev SqlM := StateM Store

instance : MonadQuery SqlM where
  query sql ps := fun st => let (r, st') := exec st sql ps; (r, st')

end Sql
