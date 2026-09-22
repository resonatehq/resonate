# pipeline — multi-stage DAG workflow

A data pipeline shaped like a DAG: a sequential download → parse stage,
then a parallel fan-out into two transforms, then a merge and emit. Every
stage is a registered function dispatched via `ctx.RPC` and backed by its
own durable promise.

```
download → parse → ┬─ transformA ─┐
                   └─ transformB ─┴─ merge → emit
```

## What this demonstrates

- **Sequential dependencies** via `Await` ordering.
- **Parallelism inside a stage**: two RPCs dispatched back-to-back, then
  awaited together — both run concurrently on whatever worker(s) pick up
  the tasks.
- **Idempotent recovery**: a crash between any two stages picks up at the
  first unsettled stage on restart; completed stages stay settled.

## Run it

Start a Resonate server on `localhost:8001`, then:

```sh
go run .
```

Expected output:

```
[runPipeline] starting workflow id=pipeline-...
  [download] example.com/doc -> 38 bytes
  [parse] 7 words
  [transformA] counting words
  [transformB] uppercased 38 chars
  [merge] combining transforms
  [emit] words=7 upper="THE QUICK BROWN FOX JUMPS OVER EXAMPLE.COM/DOC"
[runPipeline] OK: {Sent:ok}
```

The relative order of the `transformA` and `transformB` log lines may swap
between runs — they execute concurrently.

## Try killing the worker

1. Add a `time.Sleep(5 * time.Second)` at the top of `merge`.
2. Run `go run .` — wait until both `transformA` and `transformB` log lines
   appear.
3. Hit Ctrl-C to kill the worker mid-pipeline.
4. Re-run with the **same** workflow id (hard-code it for the test). The
   four earlier stages' log lines will not reappear — their promises are
   already settled — and the workflow resumes at `merge`.

## Code map

| Function      | Role                              |
|---------------|-----------------------------------|
| `runPipeline` | Orchestrator: sequences the DAG   |
| `download`    | Stage 1                           |
| `parse`       | Stage 2                           |
| `transformA`  | Stage 3 (parallel with transformB)|
| `transformB`  | Stage 3 (parallel with transformA)|
| `merge`       | Stage 4: joins both transforms    |
| `emit`        | Stage 5                           |
