# Triggers

An exploration, not a decision. What a trigger would be in this codebase, why
the port it needs already exists, and which one to build first.

## The gap

There is exactly one way into a Resonate server today: the HTTP gateway, and it
speaks the Resonate protocol. Everything that starts work — an SDK, the CLI,
the MCP proxy — is a Resonate client. Something that is not a Resonate client
cannot start a durable function, and most of the world is not a Resonate
client. It is a Slack workspace, a Discord guild, a Stripe account, a GitHub
webhook.

A **trigger** is the second kind of edge: it speaks *someone else's* protocol
and turns their events into Resonate operations. The HTTP gateway translates a
request into `process`; a trigger translates an event into `process`. Same
direction, same trust position, different wire format.

## The interface is already there

`ResonateGateway` is the port, and nothing about it is HTTP-specific:

```rust
fn new(server: Arc<dyn ResonateServer>, config: Config) -> Self;  // convention
async fn init(&self, debug: bool) -> Result<(), Unavailable>;
async fn stop(&self) -> Result<(), Unavailable>;
```

Read its doc comment with a Slack app in mind and every clause still holds. A
gateway is "driven by its own transport and has nothing we invoke per request"
— that is a websocket to Slack as much as it is a bound port. It holds the
server strongly, because it is not in the router's reference cycle and it keeps
the server alive for exactly as long as it can still accept an event. It starts
last, because delivering an event the rest of the process cannot serve is worse
than not accepting it, and it stops last, for the same reason a client would
rather have a 503 than a closed socket.

One line in `ResonateServer`'s doc comment settles the security question too:

> The trust boundary is therefore the gateway.

Signature verification — Slack's signing secret, Discord's Ed25519 header — is
the trigger's, in the same place and for the same reason `resonate-auth` is the
HTTP gateway's. Nothing downstream has to learn what a Slack request is.

So a trigger is a crate next to `resonate-gateway-http`, with plain-data config
that deserializes out of `resonate.toml`, constructed in `run_server` and
`init`ed alongside the gateway. No new port, no new lifecycle rule, no change
to `core`.

## Two operations is the whole vocabulary

A trigger needs two of the protocol's kinds, and no more.

**`promise.create`, with a `resonate:target` tag — start a function.** This is
what `resonate invoke` already sends: `param` carries base64 `{func, args,
version}` and the tag names the target. An event arriving becomes a durable
function starting.

**`promise.settle` — resume a function that is already waiting.** This is the
interesting one, and it is what separates a durable execution engine from a
webhook router. A function creates a promise, blocks on it, and survives
restarts while it waits; an inbound event resolves that promise and the
function continues on the next line. Days later, if that is how long the human
took.

```
context.promise("slack:approval:1712345678.000100")   // worker blocks here
        ↓                          ↑
   post a message              trigger settles it when someone clicks Approve
```

Inngest spells this `step.waitForEvent` and has to match events to waits.
Resonate does not need the machinery: the wait *is* a promise with an id, so a
trigger that can compute the id can resolve it. Reaper's Discord package is the
same idea in Python — `wait_for_reply(message_id)` on one side, `receive_reply`
publishing to the same key on the other.

## Idempotency comes free, and it is the pitch

Every webhook provider on this list delivers at least once and will happily
send the same event twice. The usual answer is a dedupe table with a TTL.

`promise.create` is idempotent on the promise id, and settling an
already-settled promise is a conflict rather than a second run. So if a trigger
derives the promise id from the provider's own event id —
`slack:msg:<channel>:<ts>`, `discord:reaction:<message_id>:<emoji>` — then
at-least-once delivery becomes exactly-once execution with no extra state
anywhere. The store the server already has is the dedupe table.

That is one sentence in a README and it is the strongest thing we can say about
triggers on a durable execution engine specifically.

## Mapping events to functions

The one genuinely open design question: which function does an event invoke,
with what arguments, under what id?

Inngest answers it with a per-webhook JavaScript transform that normalizes the
raw payload into a typed event, and then functions subscribe by event name.
That is a lot of surface — a language, an editor, a sandbox — for a server that
otherwise decides nothing about what an operation does.

Three cheaper answers, in the order I would reach for them:

1. **One function per trigger, event passed through verbatim.** The trigger
   invokes a single configured function with the raw payload as its argument
   and lets the worker dispatch. Policy lives in the user's code, where the
   rest of their policy already is. This is the default.
2. **A small routing table in config** — a list of `{ match, func, target, id }`
   with placeholders drawn from the event — for people who want fan-out without
   writing a dispatcher.
3. **Chat as the CLI.** For Slack and Discord specifically: `/resonate invoke
   countdown.1 --func countdown --arg 5`. The command surface already exists in
   `cli.rs`; the chat client is just another front end to it. Cute, genuinely
   useful for ops, and worth having even if it is not the main path.

Ship (1). Leave room for (2). Consider (3) as a second PR.

## The candidates

The deciding criterion is not audience size on its own. It is **how long from
`brew install` to a working durable function driven by a real event**, because
that is the demo, and a trigger nobody can try in an afternoon does not pull
anyone in. That criterion alone reorders the list.

### Slack — the widest want

Durable execution's best story is the human in the loop: approvals,
escalations, deploy gates, on-call runbooks that sleep for three days between
steps. That story happens in Slack for essentially every backend team, and the
buyer already pays for it.

- **Socket Mode is a websocket the server dials out on.** No public URL, no
  tunnel, works behind NAT on a laptop. A bot token and an app token and you
  are receiving events.
- Interactivity payloads carry `message.ts`, `action_id`, `response_url` —
  natural promise ids, and a natural place to post the result back to.
- The reply direction needs no new port: it is an HTTP call the worker makes,
  or a small outbound worker behind a `slack://` scheme if we want it durable.
- Cost: app manifests and OAuth scopes are the fiddliest onboarding of the
  three, and a workspace admin may have to approve the install.

### Discord — the shortest path

Discord is where this project's users already are; the README links the invite.
It is bot-shaped, developer-shaped, and free, and the Gateway is again a
websocket dialed outbound.

- **Cheapest onboarding of anything on this list.** Create an app, invite the
  bot, paste one token. No workspace admin, no business verification, no
  billing.
- Reactions and thread replies are near-perfect promise resolvers — ✅ on a
  message settles the promise the function is blocked on. Reaper proves the
  shape works.
- Excellent for the demo, for agent loops, for community and homelab ops.
- Cost: the Gateway is more protocol than Slack's Socket Mode — heartbeats,
  resume-after-disconnect with a session id, eventually sharding. Smaller
  enterprise pull.

### Why not WhatsApp, and why not Kafka first

**WhatsApp** has the largest raw audience and the worst first run: a Meta
Business account, a verified phone number, a publicly reachable HTTPS callback,
and a 24-hour messaging window outside which you may only send pre-approved
templates. Nobody evaluates it on a Tuesday afternoon. It is a good second
year, not a good first trigger.

**Kafka** is the most enterprise-legible and the easiest to test — a local
broker and a topic. But it reaches the fewest new people per unit of work,
since a team with Kafka already has a way to consume it, and it has no natural
"resume the promise a human is holding" story, which is the half of triggers
that is actually ours. Worth building. Not worth building first.

## Recommendation

**Slack first, Discord second** — and the second is cheap, because everything
between "an event arrived" and "a promise moved" is shared. What differs is the
wire: signature or socket handshake, payload shape, id derivation.

If the goal is instead the fastest possible working demo and the community we
already have, invert the order; the argument is close, and Discord's onboarding
really is minutes rather than an afternoon.

The shape either way:

```
crates/resonate-trigger-slack/
  src/lib.rs      Config + the ResonateGateway impl: connect, hold, drain.
  src/events.rs   Slack payload -> promise id + invoke param. The whole
                  provider-specific surface, and the only part Discord
                  would not reuse.
```

Named `trigger-`, not `gateway-`, because it does not speak the Resonate
protocol — it speaks Slack's, and turns it into two operations.
