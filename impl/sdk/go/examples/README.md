# Examples

Runnable examples for the Resonate Go SDK.

Each example lives in its own subdirectory with its own `go.mod`, so
example-only dependencies never leak into the SDK's module graph. A
`go.work` file at this directory ties every example to the local SDK
checkout (`..`), so cloning the repo and running an example needs no extra
setup.

| Example       | What it shows                                                       |
|---------------|---------------------------------------------------------------------|
| `hello`       | Minimal: register a function, run it durably, read the result.      |
| `fibbonacci`  | Recursive composition via `ctx.Run` / `ctx.RPC` / a mix.            |
| `saga`        | Multi-step workflow with compensation on failure.                   |
| `pipeline`    | DAG-shaped pipeline with parallel transforms and a merge stage.     |

## Run an example

```sh
cd examples/hello
go run .
```

Expected output:

```
hello, world!
```

## Add a new example

1. Create `examples/<name>/` with a `main.go` (`package main`).
2. Add a `go.mod`:
   ```
   module github.com/resonatehq/resonate-sdk-go/examples/<name>

   go 1.22

   require github.com/resonatehq/resonate-sdk-go v0.0.0-00010101000000-000000000000
   ```
3. Append `./<name>` to the `use (...)` block in `examples/go.work`.
4. `cd examples/<name> && go mod tidy && go run .`
