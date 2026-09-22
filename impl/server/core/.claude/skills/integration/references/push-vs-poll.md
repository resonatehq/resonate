# Push vs poll

Two independent axes get called "push" and "poll". Settle which one is under discussion
before designing anything.

## Axis 1 — how the worker receives the task

This is decided by the address scheme, and for an integration it is not really a choice:
the worker **is** the registered `ResonateWorker`, so the server calls `send` in process.
There is no transport, no delivery guarantee to reason about, and no inbound endpoint.

That is one of the reasons an integration is an in-server worker rather than an external
process: an out-of-process worker on `poll://` or `http://` adds a delivery problem
(SSE reconnects, or an authenticated public endpoint) on top of the one the task protocol
already solves.

Note the terminology trap: the `poll://` **scheme** is how an out-of-process SDK worker
receives tasks. It is unrelated to "polling" below, which is an integration asking the
*downstream* system whether a run finished.

## Axis 2 — how the worker learns the run finished

**This is the axis this skill scopes to polling**, and it is the interesting one.

### Polling (in scope)

Strengths:

- **No inbound surface.** Nothing to expose, authenticate, or firewall.
- **Self-healing.** A missed status change is corrected by the next poll. There is no state
  to lose, so a restart costs a re-poll and nothing else.
- **Uniform.** Every system that can create a run can report its status. Callbacks are a
  per-system luxury.
- **Composes with the promise deadline.** The worker is already awake and holding the clock.

Costs:

- Latency is bounded below by the poll interval.
- Load on the downstream API grows with concurrency × frequency. Back off, cap the
  interval, and use the durable-suspend shape in `lifecycle.md` when the fleet gets large.

Polling is the right default because its failure mode is **late**, and late is a much better
failure mode than **lost**.

### Push / callback (out of scope — resolve these first)

The downstream system calls back when the run finishes. Attractive — near-zero latency, no
wasted API calls — but it adds a second delivery problem on top of the one the task
protocol already solves. Do not build it until the user has answered:

1. **Who receives the callback?** The worker (needs an inbound HTTP surface in the server
   process, reachable from the downstream system), or the Resonate server directly (needs a
   promise-settling endpoint exposed to a third party — a much larger trust decision)?

2. **How is it authenticated?** Shared secret, HMAC signature, mTLS? A callback that settles
   a durable promise is a write to application state; an unauthenticated one lets anyone
   resolve any promise.

3. **How does a callback map to a promise?** The run id is derived from the promise id, so
   the reverse mapping usually exists — but confirm the callback carries the run id, and
   that it cannot be spoofed into naming a *different* promise.

4. **What happens when a callback is lost?** Every callback system drops deliveries. Without
   a reconciliation poll the promise hangs until it times out, silently converting a
   successful run into a rejected promise. In practice push almost always needs a slow poll
   behind it — which means building the poll integration anyway.

5. **Duplicated or out-of-order callbacks?** `task.fulfill` is version-fenced, so a second
   callback gets `409` — but only while the worker still holds the lease. A callback for an
   already-settled promise must be discarded, not retried.

6. **A callback that arrives before create returns?** Fast runs finish before the create
   response is processed. The handler must tolerate a callback for a task the worker has not
   finished acquiring.

7. **Who owns the endpoint's availability?** If the server is down when the callback fires,
   is it retried, and for how long? A downstream system with no callback retry turns every
   deploy into lost completions.

8. **Does the promise deadline still govern?** With nobody polling, nothing notices a run
   that stalls. The server still times the promise out — verify that is intended rather than
   accidental.

The pragmatic middle ground, worth proposing whenever push comes up: **poll as the source of
truth, push as an accelerator.** The callback does nothing but trigger an immediate poll.
Correctness comes entirely from the poll loop, so a lost, duplicated, out-of-order or
spoofed callback costs at most one wasted status check. If push is added to this skill
later, that is the shape it should take.
