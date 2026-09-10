# valid/ — conformance

Two checkers built on the specification, one input format:

- [`lean/`](lean) — the trace checker. `lake exe checktrace < trace.ndjson`
  asks whether ANY schedule of internal steps explains the recorded events,
  against the specification itself.
- [`porc/`](porc) — a Go port of the abstract machine, wired up for the
  [porcupine](https://github.com/anishathalye/porcupine) linearizability
  checker. `lincheck < trace.ndjson` answers under both read disciplines.

## The trace format

NDJSON — one JSON object per external call, in observation order:

```json
{"kind":"promise.create","now":1000,"req":{…},"res":{"kind":"promise.create","head":{…},"data":{…}}}
```

| field | contents |
|---|---|
| `kind` | the request kind: `promise.get` / `create` / `settle` / `register_callback` / `register_listener`, `task.get` / `create` / `acquire` / `fence` / `heartbeat` / `suspend` / `fulfill` / `release` / `halt` / `continue`, `schedule.get` / `create` / `delete` |
| `now` | the instant of the observation, in milliseconds — non-decreasing across the file |
| `req` | the `data` the client sent |
| `res` | the whole `{kind, head, data}` envelope the server returned |

This is resonate's wire format, not a shape invented here — a capture is
teed traffic. Two rules for producing one:

- **Internal steps are NOT in the file and must not be.** The specification
  deliberately leaves the τ schedule open; the checkers recover the internal
  steps by existential search.
- **One clock.** `now` and the deadlines the promises carry must come from
  the same clock. Against resonate that means sending `debug.start` first
  and stamping `resonate:debug_time` on every request — otherwise the
  background loops judge small deadlines against wall clock and expire
  whatever is pending.

Two decoding notes, mirrored in [`lean/json.lean`](lean/json.lean): tags
compare key-sorted (JSON objects carry no order), and a non-2xx response
carries `data` as a bare string message rather than an object.

## Producing traces

The `scenarios` binary ([`work/go`](../work/go)) drives the Go SDK's durable
functions against a real server and records the trace; it also writes a
`.history` of call/return intervals for `porc/cmd/conccheck`, which searches
orders rather than verifying the recorded one. Any other producer works as
long as it emits the format above — it is tee'd traffic, so a proxy on the
server's port suffices.

A corpus of recorded runs from resonate v0.9.8 formerly lived in `traces/`
here, together with the `capture.py` proxy that recorded it; both are
recoverable from git history.
