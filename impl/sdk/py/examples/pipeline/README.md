# pipeline — multi-stage DAG workflow

A data pipeline shaped like a DAG: a sequential download → parse stage, then a
parallel fan-out into two transforms, then a merge and emit. Every stage is a
registered function dispatched via `ctx.rpc` and backed by its own durable
promise.

```
download → parse → ┬─ transform_a ─┐
                   └─ transform_b ─┴─ merge → emit
```

## What this demonstrates

- **Sequential dependencies** via `await` ordering.
- **Parallelism inside a stage**: two RPCs dispatched back-to-back, then
  awaited together — both run concurrently on whatever worker(s) pick up the
  tasks.
- **Idempotent recovery**: a crash between any two stages picks up at the first
  unsettled stage on restart; completed stages stay settled.

## Run it

Start a Resonate server on `localhost:8001` (`resonate dev`), then:

```sh
uv run python examples/pipeline/main.py
```

Expected output:

```
[run_pipeline] starting workflow id=pipeline-...
  [download] example.com/doc -> 46 bytes
  [parse] 7 words
  [transform_a] counting words
  [transform_b] uppercased 46 chars
  [merge] combining transforms
  [emit] words=7 upper='THE QUICK BROWN FOX JUMPS OVER EXAMPLE.COM/DOC'
[run_pipeline] OK: sent='ok'
```

The relative order of the `transform_a` and `transform_b` log lines may swap
between runs — they execute concurrently.

## A note on replay

The SDK runs an orchestrator with a
**suspend-and-replay** model: `run_pipeline` re-executes from the top each time
it awaits a not-yet-settled future. So all side effects (the log lines here)
live in the leaf stage functions, which settle exactly once and are never
re-run. If you moved a `print` into `run_pipeline`, you would see it repeat on
every replay.

## Try killing the worker

1. Add an `await asyncio.sleep(5)` at the top of `merge`.
2. Run the example — wait until both `transform_a` and `transform_b` log lines
   appear.
3. Hit Ctrl-C to kill the worker mid-pipeline.
4. Re-run with the **same** workflow id (hard-code it for the test). The four
   earlier stages' log lines will not reappear — their promises are already
   settled — and the workflow resumes at `merge`.

## Code map

| Function       | Role                               |
|----------------|------------------------------------|
| `run_pipeline` | Orchestrator: sequences the DAG    |
| `download`     | Stage 1                            |
| `parse`        | Stage 2                            |
| `transform_a`  | Stage 3 (parallel with transform_b)|
| `transform_b`  | Stage 3 (parallel with transform_a)|
| `merge`        | Stage 4: joins both transforms     |
| `emit`         | Stage 5                            |
