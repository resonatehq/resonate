# Resonate Specification

The Resonate protocol, written down once, precisely enough to check things
against.

It is an executable **abstract machine** in Lean 4 — a state, a set of
effects, and one transition per request — together with a **catalogue of
properties** that every run of it satisfies, and the tools to hold a real
server to them.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/am-b.png">
    <img src="docs/am-w.png" alt="Abstract Machine">
  </picture>
</p>

## Why

An implementation needs an answer to "is this behaviour correct?" that is
not another implementation. This repository is that answer, in three parts:

- **the machine** says what the server does — one step per request, plus the
  steps it takes on its own initiative;
- **the catalogue** says what must be true of it — 94 properties, each a
  decidable predicate on a state or on a pair of consecutive states;
- **the checker** takes a trace recorded from a real server and asks whether
  the machine can account for it.

The properties are the portable part. They are written against the
specification, not against any implementation, so a violation is a bug
report rather than a difference of opinion.

## Layout

| | |
|---|---|
| [`spec/01-protocol`](spec/01-protocol) | the wire surface — records, requests, responses, validation |
| [`spec/02-abstract`](spec/02-abstract) | the machine: state, effects, handlers, the system definition, and the property catalogue |
| [`spec/04-theorems`](spec/04-theorems) | what is proved about it, and the harnesses that evaluate it |
| [`valid/`](valid) | the trace checker — `lake exe checktrace < trace.ndjson` |
| [`work/`](work) | traffic generators that drive real servers and record traces |

Every Lean file opens with a header saying what that piece is for and why
it is shaped the way it is; `valid/` and the two `work/` generators carry
their own READMEs.

## Status

Every entry of the catalogue holds over 1 464 enumerated scripts, under
both read disciplines, checked by kernel `decide` at build time. Of the 94
entries, 31 are proved outright — true at every instant of every run, no
bound and no sample. The remaining 63, and the theorem that collects them
(`valid_implies_legal`), are stated and open.

[`spec/implementation-questions.md`](spec/implementation-questions.md) is the
companion for implementers: every question an implementation has to answer,
paired with the property that fires when it is answered wrongly.

## Build

```
lake build spec    # the specification — the fast loop
lake build valid   # the trace checker
lake build         # everything, including the decide sweeps (minutes)
```

Lean 4, no Mathlib.
