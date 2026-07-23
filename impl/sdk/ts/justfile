default:
    just --list

# Build, type-check, lint, format, and test.
build:
    npm run build

type-check:
    npm run type-check

check:
    npm run check

fmt:
    npm run fmt

test:
    npm test

# Deterministic simulation testing.
dst:
    npm run dst

dst-diff:
    npm run dst:diff

# Run every example against a Resonate server on localhost:8001.
# Start one first: `resonate dev`. Override with RESONATE_URL.
examples: examples-generator examples-async

examples-generator:
    npx tsx examples/generator/hello.ts
    npx tsx examples/generator/fibonacci.ts --mode run --n 12
    npx tsx examples/generator/fibonacci.ts --mode rpc --n 12
    npx tsx examples/generator/fibonacci.ts --mode mix --n 12
    npx tsx examples/generator/rpc.ts
    npx tsx examples/generator/versioning.ts
    npx tsx examples/generator/error-handling.ts --mode run --error none
    npx tsx examples/generator/error-handling.ts --mode run --error taken
    npx tsx examples/generator/error-handling.ts --mode run --error value
    npx tsx examples/generator/error-handling.ts --mode rpc --error none
    npx tsx examples/generator/error-handling.ts --mode rpc --error taken
    npx tsx examples/generator/error-handling.ts --mode rpc --error value
    npx tsx examples/generator/human-in-the-loop.ts --decision approve
    npx tsx examples/generator/human-in-the-loop.ts --decision reject
    npx tsx examples/generator/recovery.ts
    npx tsx examples/generator/detached.ts
    npx tsx examples/generator/polling.ts
    npx tsx examples/generator/structured-concurrency.ts
    npx tsx examples/generator/retries.ts
    npx tsx examples/generator/pipeline.ts
    npx tsx examples/generator/saga.ts
    npx tsx examples/generator/saga.ts --fail hotel
    npx tsx examples/generator/saga.ts --fail charge

examples-async:
    npx tsx examples/async/hello.ts
    npx tsx examples/async/fibonacci.ts --mode=run --n=12
    npx tsx examples/async/fibonacci.ts --mode=rpc --n=12
    npx tsx examples/async/fibonacci.ts --mode=mix --n=12
    npx tsx examples/async/rpc.ts
    npx tsx examples/async/versioning.ts
    npx tsx examples/async/error-handling.ts --mode=run --error=none
    npx tsx examples/async/error-handling.ts --mode=run --error=taken
    npx tsx examples/async/error-handling.ts --mode=run --error=value
    npx tsx examples/async/error-handling.ts --mode=rpc --error=none
    npx tsx examples/async/error-handling.ts --mode=rpc --error=taken
    npx tsx examples/async/error-handling.ts --mode=rpc --error=value
    npx tsx examples/async/human-in-the-loop.ts --decision=approve
    npx tsx examples/async/human-in-the-loop.ts --decision=reject
    npx tsx examples/async/recovery.ts
    npx tsx examples/async/detached.ts
    npx tsx examples/async/polling.ts
    npx tsx examples/async/structured-concurrency.ts
    npx tsx examples/async/retries.ts
    npx tsx examples/async/pipeline.ts
    npx tsx examples/async/saga.ts
    npx tsx examples/async/saga.ts --fail=hotel
    npx tsx examples/async/saga.ts --fail=charge
