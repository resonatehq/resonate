# Knowledge

This file gives LLMs guidance for writing correct, idiomatic code using the Resonate TypeScript SDK.  

## Project Overview

Resonate is a durable execution platform based on functions and promises. Resonate lets users write durable, fault-tolerant code in an imperative and familiar style. Functions can compose recursively and each function is backed by a durable promise that persists across interruptions.

## Run
- Install: `npm install @resonatehq/sdk`
- Import: `import { Resonate, type Context } from "@resonatehq/sdk";`

## Resonate

### Instantiation

Create an instance of the Resonate class with the constructor.

```ts
const resonate = new Resonate({
  url: "http://localhost:8001",  // optional server url
  pid: "worker-1",               // optional pid
  group: "default",              // optional group
});
```

Notes:

- All arguments to the constructor are optional.
- If a url is provided the instance will connect to a remote Resonate server, if not provided the instance will use a local server implementation.
- Using a local or remote server does not change the behavior of Resonate.
- Promises are only durable (survives restarts) when using a remote server.

### Registering functions

Registered functions **must** accept a `Context` object as their first parameter. You may register a function, an async function, or a generator. You must never register an async generator.

```
function greet(ctx: Context, name: string) {
  return `Hello, ${name}!`;
}

resonate.register(greet);
```

Optionally you can provide a name when registering if you want to register the function under a different name.

```ts
resonate.register("hello", greet);
```

You may optionally provide a function version.

```ts
resonate.register(greetV2, { version: 2 });
```

### Running functions

Resonate offers the following ways to invoke functions:

1. **`run(id, func, ...args)`** — schedules a registered function on this instance and **waits** for a result
2. **`beginRun(id, func, ...args)`** — schedules the run on this instance and returns immediately with a handle that can be used to await the result later
3. **`rpc(id, func, ...args)`** — schedules a registered function on a remote instance and **waits** for a result
4. **`beginRun(id, func, ...args)`** — schedules the run on a remote instance and returns immediately with a handle that can be used to await the result later

Notes:

- The id parameter uniquely identifies thie execution. If another run uses the same id it will join the execution and get the same result.
- The func must be registered.
- The func can be either a string that references a registered function or a reference to the function itself.
- The args are the function arguments with Context omitted, this is provided by Resonate. All arguments **must** be json serializable.
- The return value of the function **must** be json serializable.

These function calls should always be awaited (eg: `await ctx.run(...)`, `await ctx.beginRpc(...)`). The need to await `run`/`rpc` is self evident; `beginRun`/`beginRpc` must be awaited to ensure the corresponding durable promise is created in addition to getting a handle.

Example:

```ts
const result = await resonate.run("greet-001", "greet", "Bob");
console.log(result)  // "Hello, Bob!"

const result = await resonate.rpc("greet-002", "greet", "Alice");
console.log(result)  // "Hello, Alice!"

const handle = await resonate.beginRun("greet-003", "greet", "Bob");
const result = await handle.result();
console.log(result)  // "Hello, Bob!"

const handle = await resonate.beginRpc("greet-004", "greet", "Alice");
const result = await handle.result();
console.log(result)  // "Hello, Alice!"
```

### Options

`resonate.options({})` returns an `Options` object you can pass as the last argument to `run`, `rpc`, `beginRun`, and `beginRpc`. Resonate supports the following options:

- **target** - the rpc call target, can be a url (eg: http://localhost:8080) or a group name (eg: gpu), defaults to "default"
- **timeout** - the time at which the execution times out, defaults to 24 hours
- **version** - the function version to use, defaults to 0 (latest)

### Join an existing execution

Use `get(id)` when you know a durable promise already exists:

```ts
const handle = await resonate.get<string>("greet-001");
const result = await handle.getResult();
```

### Dependency injection

You can attach dependencies to a Resonate instance. These dependencies can be used later with Context.

```ts
resonate.setDependency("db", myDb);
```

## Context

A `Context` instance is provided only as an **argument to registered functions**. You should not construct it yourself.

Example:
```ts
function* foo(ctx: Context) {}
```

### Durable function invocation

Context offers the following ways to invoke functions. All function invocations must be preceded by `yield*` and therefore must occur in generator functions.

- **`ctx.run(func, ...args)`** - run a function on this instance and **wait** for a result
- **`ctx.beginRun(func, ...args)`** - run a function on this instance and get a promise that can be used to await the result later
- **`ctx.rpc(func, ...args)`** - run a function on a remote instance and **wait** for a result
- **`ctx.beginRpc(func, ...args)`** - run a function on a remote instance and get a promise that can be used to await the result later

Notes:

- The provided func can be either a string that references a registered function or a reference to a function itself.
- For `run` and `beginRun` if a string is provided the function **must** be registered. If a function reference is provided the function **does not** need to be registered.
- For `run` and `beginRun` the function parameters **do not** need to be json serialiable.
- For `rpc` and `beginRpc` if a string is provided the function **does not** need to be registered. If a function reference is provided the function **must** be registered.
- For `rpc` and `beginRpc` the function parameters **must** be json serialiable.
- For all functions the return value **must** be json serializable.

Use generator functions for recursively invoking additional functions (run, beginRun, rpc, beginRpc, detached, promise, sleep, etc). Use async functions to perform side effects, async function **cannot** invoke functions recursively and therefore represent the leaves of the call graph.

Resonate enforces structured concurrency, that is, all recursive function calls are automatically awaited before a value is returned and the enclosing durable promise is completed. If you need to fire off a function that is not part of the structured concurrency use `ctx.detached(...)` (see below).

Example:

```ts
function* foo(ctx: Context) {
  const result1 = yield* ctx.run(greet, "Bob")
  console.log(result1)  // "Hello, Bob!"

  const result2 = yield* ctx.rpc(greet, "Alice")
  console.log(result2)  // "Hello, Alice!"

  const promise1 = yield* ctx.beginRun(greet, "Bob")
  const result3 = yield* p;
  console.log(result3)  // "Hello, Bob!"

  const promise2 = yield* ctx.beginRpc(greet, "Alice")
  const result4 = yield* p;
  console.log(result4)  // "Hello, Alice!"
}
```

### Detached execution

Use `ctx.detached(...)` to fire off a durable invocation without including it in the structured concurrency.

```ts
yield* ctx.detached(greet, "Eve");
```

### Manual resolution

Use `ctx.promise(...)` to create a promise that can be resolved elsewhere. This is useful for interacting with external promises, or human based control gates.

```ts
const promise = yield* ctx.promise();

// manually resolve the promise (this may occur anywhere)
resonate.promises.resolve(promise.id);
```

### Sleep

You may wait for arbitrary lengths of time with `ctx.sleep(...)`. During this time your function is suspended and does not consume resources.

```ts
yield* ctx.sleep(15_000); // sleep for 15s
```

### Options

`context.options({})` returns an `Options` object you can pass as the last argument to `run`, `rpc`, `beginRun`, `beginRpc`, and `detached`. Context supports the following options:

- **id** - the id of the corresponding durable promise, defaults to a simple sequence id
- **target** - the rpc call target, can be a url (eg: http://localhost:8080) or a group name (eg: gpu), defaults to "default"
- **timeout** - the time at which the execution times out, defaults to 24 hours
- **version** - the function version to use, defaults to latest

### Accessing dependencies

Get dependencies that were set on the `Resonate` instance:

```ts
const db = ctx.getDependency<MyDb>("db");
```

### Assertions

Context provides helpers to stop a durable execution. Like function invocations these calls must be preceded by `yield*`. Use these methods to ensure invariants are not violated.

- **`ctx.assert(condition, msg?)`** - stops the execution if the condition evaluates to `false`
- **`ctx.panic(condition, msg?)`**  - stops the execution if the condition evaluates to `true`

---

## Codegen rules for LLMs

When generating TypeScript for this repo:

1. Prefer an id based on the semantics of the use case, if appropriate a timestamp or a uuid may be used.
2. The first argument of functions must always be **Context**.
3. Prefer using generator functions with the Resonate class unless the use case requires a single side effect.
4. Use async functions for leaf functions and prefer leaf functions that perform a single side effect.
5. Orchestrate leaf functions using generator functions, never use async generator functions.
6. Prefer code that:
   - constructs a `Resonate` client once
   - uses a local server by not specifying a url, unless otherwise instructed
   - `register()` functions on startup and do not provide a name (use function name)
   - then `run()` or `rpc()` by reference
7. Don’t invent APIs. If a method isn’t in this file, don’t use it without confirming in docs.
8. Keep examples minimal and runnable.

