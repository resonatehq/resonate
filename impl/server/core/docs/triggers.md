# Triggers

An exploration, not a decision. Scope: **starting a workflow that is not
running yet.** Nothing here is about resuming one.

## Resonate already has a trigger

A schedule is one. Look at what the record actually holds:

```
id              the schedule
cron            when it fires
promiseId       a TEMPLATE — "run-{{.timestamp}}"
promiseTimeout
promiseParam    {func, args, version}, base64 in `data`
promiseTags     must include resonate:target   (types.rs:781)
```

When the clock fires, the engine substitutes `{{.id}}` and `{{.timestamp}}`
into the template and creates the promise. That is the whole mechanism, and it
is already implemented four times over — sqlite, postgres, mysql, oracle.

So the question is not "what would a trigger be". A trigger is **a stored rule
that turns an occurrence into a `promise.create`**, and Resonate ships one
already, where the occurrence is the clock. What is missing is the other
occurrence: an event from somewhere that is not a clock.

That reframing does most of the design work:

```
schedule = { cron,   promiseId, promiseTimeout, promiseParam, promiseTags }
trigger  = { source, promiseId, promiseTimeout, promiseParam, promiseTags }
```

Same right-hand side. Only the left changes.

## One operation

A trigger needs exactly one of the protocol's kinds: `promise.create`, with a
`resonate:target` tag. That is byte-for-byte what `resonate invoke` puts on the
wire (`cli.rs:1050`) — `param` carries base64 `{func, args, version}`, the tag
names the target.

Nothing else. No new kind, no new engine call, no change to `core`.

## The interface is the gateway's

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
last, because taking an event the rest of the process cannot serve is worse
than not taking it, and it stops last, for the reason the comment already
gives.

One line in `ResonateServer`'s doc comment settles the security question too:

> The trust boundary is therefore the gateway.

Signature verification — Slack's signing secret, Discord's Ed25519 header — is
the trigger's, in the same place and for the same reason `resonate-auth` is the
HTTP gateway's. Nothing downstream has to learn what a Slack request is.

So: a crate next to `resonate-gateway-http`, plain-data config out of
`resonate.toml`, constructed in `run_server`, `init`ed alongside the gateway.

## The chat mapping

A trigger is one function:

```
Event -> PromiseCreateData { id, timeout_at, param, tags }
```

There is no template language, no filters, and no argument parsing. The whole
mapping is fixed:

```
     command ──trim '/'──►  func
        text ──verbatim──►  args[0]
  trigger_id ───────────►   id
      config ───────────►   version, timeout, target
```

### Why the text is not parsed

A Slack slash command has **no parameter schema**. `text` is one raw string —
everything typed after the command, unparsed, unvalidated, untyped. Slack does
not know that `/backfill` takes a collection and a date, and cannot tell a user
when they leave one out.

So `["orders", "2024-01-01"]` is not something Slack hands over. It is
something a trigger would *invent*, by splitting on whitespace and hoping. That
guesses at a function's arity — `/backfill orders` yields one argument where
two were wanted — and the failure surfaces deep inside the worker instead of at
the edge. Quoting, flags, and empty trailing fields each make it worse.

Everything built on top of that guess inherits its brittleness: a `words`
filter, a literal-array config with native JSON types, a parameter schema
declared in TOML, a modal opened to collect typed inputs. Each is more
machinery defending a split that should not happen.

**The string goes through whole.** The worker parses its own grammar, in its
own language, where it can also test it.

The same rule settles Discord, which *does* deliver typed options: the options
object goes through as one argument, unflattened. Two providers, one rule —
hand the user's content to the worker as it arrived and decide nothing about
it. Which also means the structured/unstructured difference between the two
platforms stops being an argument about which to build first.

### What is left to configure

Almost nothing, and that is the point:

```toml
[triggers.slack]
app_token = "xapp-..."          # Socket Mode
bot_token = "xoxb-..."
target    = "poll://any@default"
timeout   = "1h"
```

Every command the Slack app declares invokes the function of the same name.
There is no per-command block to write and no registry to keep in sync, because
Slack already holds one: an app only receives the commands its own manifest
declares, so the manifest *is* the allowlist. A per-command override —
a different `func`, a longer `timeout` — stays available for the cases that
need it, and is expected to be rare.

### `id`

Always `trigger_id`, and it is worth saying why the trigger does not let this
be templated from the payload.

An id a trigger mints is three things at once: the dedupe key; the
`resonate:origin` of an entire call graph (`<this>:1`, `<this>:1.1`, …); and a
value in a reserved grammar, since `origin()` splits an id at its first `':'`
(`types.rs:399`). `validate_schedule_create_data` already refuses a `':'` in a
schedule id for exactly this reason, and says so: an origin holding a colon
"is unrepresentable: no id could ever split back to it".

`text` therefore must not reach it — and for a better reason than escaping.
An id built from what someone typed is *semantically* wrong: two people running
`/backfill orders` would mint the same id, and the second would silently
receive the first's promise instead of starting their own. That is the dedupe
mechanism working exactly as designed, on the wrong key.

`trigger_id` is Slack's own identifier for the interaction, and it is
already `[A-Za-z0-9.]`. Check the charset anyway before using it — the check
never fires today, and it is there so that a provider changing a format cannot
smuggle a `':'` into an origin.

### `param`

The shape is what `resonate invoke` already sends and what the SDKs already
decode:

```
param.data = base64(json { func: "backfill", args: ["orders 2024-01-01"], version: 1 })
```

`param.headers` is `Option<HashMap<String, String>>` and the CLI leaves it
empty. It is the natural home for provenance the *worker* needs but the
function *signature* should not carry — `slack.response_url` above all, since
that is how a result gets posted back half an hour later.

> **Open question.** Whether the SDKs surface `param.headers` to a function.
> If they do not, `response_url` has to travel as an argument or a tag instead.
> Worth settling before writing the crate.

### `tags`

`resonate:target` is required — the rule `validate_schedule_create_data`
already enforces for schedules, and `op_promise_create` validates the address
besides.

Everything else is ours, and worth spending: tags are searchable, so stamping
provenance

```
slack:command = "/backfill"   slack:channel_id = "C214…"   slack:user_id = "U214…"
```

makes "every workflow anyone started from #ops this week" a `promise.search`
rather than a feature. It falls out of the mapping for free, which is reason
enough to make it the default rather than an option.

### `timeout_at`

`now + timeout`, with `parse_duration` from `cli.rs` reading the config.

### What happens when it goes wrong

With no parsing left, there are only two outcomes, and one of them is not an
error:

- **`promise.create` returns 200.** This is also what a *duplicate* delivery
  returns: `op_promise_create` finds the existing promise and returns its record
  rather than conflicting (`oracle.rs:373`). So the trigger never special-cases
  dedupe — it always gets a promise back, and tells a repeat from a first run by
  whether `createdAt` belongs to this delivery. "Already running, id
  `backfill-…`" is a better answer to a double-tapped command than a second
  workflow.
- **`Unavailable`.** Do not ack. Slack retries, and the retry is safe precisely
  because the id names the interaction rather than the delivery. The provider's
  retry becomes ours, for free, and that is worth choosing on purpose rather
  than discovering.

There is deliberately no third case for a malformed command, because with the
text passed through whole there is nothing left to malform.


## The queue trigger: filter, then transform

A queue is a different trigger from a chat command, and the differences run
deep enough that the pass-through rule above does **not** carry over.

|                | slash command            | queue record                       |
|----------------|--------------------------|------------------------------------|
| which function | the command names it     | nothing in the record names it     |
| relevance      | every command is for us  | most of the topic is not           |
| payload        | one unparsed string      | structured, schema'd, nested       |
| arrival        | a human, occasionally    | a firehose, continuously           |

Splitting Slack's `text` was brittle because it invented structure that was
never sent. A Kafka or JetStream record *has* structure — the producer put it
there deliberately — so reading fields out of it is not a guess. And a topic
carries messages that are none of our business, so a filter is not a
convenience here; without one the trigger is wrong.

So: **filter, then transform.** Both belong.

### The language: CEL for both

```toml
[[triggers.nats]]
stream   = "ORDERS"
consumer = "resonate-fulfil"          # durable, JetStream

filter = 'data.type == "order.placed" && data.amount > 100'
func   = '"fulfil"'
args   = '[data.order_id, data.amount]'

target  = "poll://any@default"
timeout = "24h"
```

CEL — Google's Common Expression Language, `cel-interpreter` — rather than jq,
Rhai, Lua or a template engine, for four reasons that are specific to this
being a queue:

1. **It is not Turing-complete and is guaranteed to terminate**, in linear
   time. The filter runs on *every message at topic throughput*; an expression
   that can loop is an expression that can wedge a partition. jq can
   (`until`, `repeat`, `recurse`), so embedding jaq means a step budget and a
   watchdog per message. CEL structurally cannot.
2. **It compiles at startup.** A malformed expression is a startup failure that
   names itself — the same discipline as the gateway reading its key material
   in `init`, so "a bad key path stops the process here rather than surfacing
   later as a request nobody can authenticate".
3. **It constructs.** CEL builds lists and maps, so the transform needs no
   second language: `func` and `args` are just more expressions.
4. **It is side-effect free**, so a filter cannot reach the network or the
   disk, and there is no sandbox to get wrong.

> **Caveat.** `cel-interpreter` is at 0.10 and was last published in July 2025
> — healthy download numbers, but a year without a release. Worth reading the
> repository before committing to it. `jaq-core` is the fallback, with the
> termination cost above accepted deliberately.

### The hard part is the offset, not the language

There are two systems here — the queue's cursor and Resonate's store — and no
way to move both atomically. So the order is the design:

```
poll → filter → transform → promise.create → ack / commit
                                      ↑
                    a crash here redelivers, and the id makes it a no-op
```

**Create, then commit.** Committing first is at-most-once and silently drops
work on any crash. Creating first is at-least-once, and at-least-once plus an
id derived from the message is exactly-once *execution*.

Note what that does to the idempotency argument below. For a webhook it is a
nice property. Here it is load-bearing: it is the only thing standing between a
consumer restart and a duplicate workflow.

### Which id

- **Kafka**: `topic-partition-offset`. Unique by construction, needs no config,
  and correct without the user thinking about it.
- **JetStream**: the stream sequence, which plays the same role.
- **Override**: a business key from the payload, `"order-" + data.order_id`.
  This is a *different* guarantee, not a nicer spelling of the same one — it
  additionally collapses a producer that published the same event twice at two
  offsets. Some teams want exactly that and some would call it lost data, so it
  is a deliberate choice rather than a default.

### Four things that will bite

**Core NATS cannot do this.** It has no message id and no redelivery — publish
and hope. The trigger should require **JetStream** and refuse a core-NATS
subject at startup, rather than quietly losing every message it was mid-way
through when the process died.

**Ordering does not survive.** Kafka orders within a partition; promises, once
created, run concurrently and finish in whatever order they finish. The trigger
preserves no ordering into execution, and people will assume otherwise unless
it is written down. A workflow needing per-key serialization owns that itself.

**There is no batch create.** The protocol dispatches one operation per request
(`oracle.rs:271`), so every message is a round trip and a database write. The
consumer therefore has to be bounded and to **pause on backpressure rather than
buffer** — a topic faster than the engine's write rate is a misconfiguration to
report, not a backlog to absorb. A batched `promise.create` is the obvious
optimization and it is a protocol change, so: noted, not done.

**Poison messages must not block a partition.** A record that fails the
transform gets skipped, logged and counted, with a config knob for teams that
would rather stop than skip. The alternative is one malformed message halting a
topic forever.

And the standing rule from `ResonateWorker::init` applies unchanged: under
`debug` the clock belongs to the caller, so a polling consumer must not start.

### NATS first, then Kafka

An ecosystem fact decides this one, and it is not close:

| | crate | 90-day downloads | |
|---|---|---|---|
| NATS  | `async-nats` 0.50 | 5.9M | official, **pure Rust** |
| Kafka | `rdkafka` 0.39    | 6.3M | wraps **librdkafka (C)** |
| Kafka | `rskafka` 0.6     | 416k | pure Rust, but **no consumer groups, no offset tracking** |

Resonate ships as a single binary people install with `brew`. `rdkafka` puts a
C library in that build and in every cross-compilation target, and the pure-Rust
alternative explicitly does not do the two things a trigger needs most. NATS has
no such trade: `async-nats` is first-party, pure Rust, and JetStream gives
sequence numbers, durable consumers and redelivery semantics that map onto the
design above without adaptation.

So NATS first — and it is the smaller, cheaper implementation as well as the
better-behaved dependency. Kafka second, once the consumer core exists and only
the client differs, at which point the librdkafka cost is a considered decision
rather than the price of entry.

## Idempotency is the pitch

Every provider on this list delivers at least once and will re-send on any
timeout. The usual answer is a dedupe table with a TTL. For a trigger, a
duplicate delivery is a duplicate *workflow* — the failure everyone
immediately understands.

`promise.create` is idempotent on the promise id. So an id taken from the
provider's own identifier for the interaction makes at-least-once delivery into
exactly-once kickoff, with no extra state anywhere. The store the server already
has is the dedupe table.

Which identifier, precisely, because it is easy to get wrong: **a slash command
has no `event_id`.** That field belongs to the Events API. A command carries
`trigger_id`, unique per invocation, and — over Socket Mode — an `envelope_id`
on the wrapper. `trigger_id` is the right one, because it names the user's
interaction rather than one delivery attempt of it. Whether Slack reuses it
across a retried delivery is the one thing here worth confirming empirically
before the pitch depends on it.

That is one sentence in a README, and it is the strongest thing we can say
about triggers on a durable execution engine specifically.

## How a Slack command actually arrives

Two delivery modes, and the choice between them is most of what the crate looks
like.

**Over HTTP, to a Request URL.** A `POST`, and — unlike the Events API, which
is JSON — the body is `application/x-www-form-urlencoded`:

```
POST /slack/commands
Content-Type: application/x-www-form-urlencoded
X-Slack-Request-Timestamp: 1712345678
X-Slack-Signature: v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503

token=deprecated&team_id=T0001&team_domain=acme
&channel_id=C2147483705&channel_name=ops
&user_id=U2147483697&user_name=deprecated
&command=%2Fbackfill&text=orders+2024-01-01
&api_app_id=A123&is_enterprise_install=false
&response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2F1234%2F5678
&trigger_id=13345224609.738474920.8088930838d88f008e0
```

Verification is the trust-boundary work: HMAC-SHA256 over the literal string
`v0:{timestamp}:{raw_body}` keyed by the signing secret, hex, prefixed `v0=`,
compared in constant time — and reject a timestamp more than five minutes old,
which is the replay defence. It has to run on the **raw** bytes, before any
form parsing, which is worth knowing early because it constrains how the
handler is written.

Then: 3 seconds to respond, `response_url` good for 30 minutes and five posts,
`trigger_id` good for about 3 seconds and only for opening a modal.

**Over Socket Mode, on a websocket we dial.** `apps.connections.open` with an
app-level token (`xapp-`, scope `connections:write`) returns a `wss://` URL;
connect to it and the same payload arrives wrapped:

```json
{
  "envelope_id": "57d6a792-4d35-4d0b-b6aa-3361493e1caf",
  "type": "slash_commands",
  "accepts_response_payload": true,
  "payload": { "command": "/backfill", "text": "orders 2024-01-01",
               "trigger_id": "1334...", "channel_id": "C2147483705", "...": "..." }
}
```

Acknowledge by sending the `envelope_id` back over the socket within 3 seconds,
or Slack retries. Slack also sends `hello` on connect and `disconnect` when it
wants the client to reconnect, so a real implementation needs a reconnect loop
with backoff.

**Socket Mode is the one to build.** No public URL and no tunnel, so it runs on
a laptop; and the websocket is authenticated by the app-level token, so there
is no signature verification, no five-minute clock skew window, and no raw-body
handling. It also fits `ResonateGateway` exactly as written — `init` dials and
holds, `stop` drains — where the HTTP mode would want to mount routes on a
listener the gateway crate already owns, and inverting that dependency is a
design argument we do not have to have yet.

## The three-second ack

Slack requires a response to a slash command within 3 seconds; Discord the same
for an interaction. This is usually a nuisance and here it is a gift: starting
a durable workflow *is* a sub-millisecond operation, and "started, id
`backfill-1712345678`" is a genuinely useful ack. Both platforms then hand back
a `response_url` / interaction token the worker can post the real result to,
minutes or hours later. The awkward shape for everyone else is the natural
shape for us.

## Answering a slash command

Three ways back, and they belong to two different answerers.

**The ack — the trigger's.** Within three seconds. Over Socket Mode the ack
envelope may itself carry a `payload` when Slack sets `accepts_response_payload`
on the request; over HTTP it is the response body. Either way it is a message
with a `response_type` of `ephemeral` (only the person who typed it) or
`in_channel` (everyone). This is where "started, id `backfill-…`" goes, and
where a repeat delivery says "already running" instead.

**The result — the worker's**, and it has two options:

- **`response_url`**, posted to as JSON. No token needed — the URL is the
  credential — which makes it the easy one. But it is valid for **30 minutes
  and at most 5 posts**.
- **`chat.postMessage`** with the bot token and a `channel_id`. No expiry, no
  limit.

That 30-minute window is the wrinkle, and it points the wrong way for us:
Resonate exists to run functions that outlive it. A workflow that sleeps for a
day and then reports cannot use `response_url` at all. So the provenance a
trigger stamps should be the parts that never expire — `channel_id`, `user_id`,
`command` — with `response_url` carried as a convenience for the fast cases
rather than as the mechanism.

Which also means the worker needs a Slack token to answer at all. Two ways to
give it one, and the boring one is right: the worker calls `chat.postMessage`
itself, with its own token, in its own code — the same way it would call any
other API. An outbound `slack://` worker behind the router would make posting
durable and retried, and is worth wanting eventually, but it is a second crate
and a second decision. Not this one.

## The candidates

The deciding criterion is not audience size on its own. It is **how long from
`brew install` to a workflow started by a real event**, because that is the
demo, and a trigger nobody can try in an afternoon pulls nobody in.

### Slack — the widest want

"Kick off a workflow from chat" is overwhelmingly an internal-ops use case:
backfills, deploys, replays, re-runs, incident checklists. That work happens in
Slack for essentially every backend team, which is also Resonate's audience,
and the buyer already pays for Slack.

- **Slash commands are typed invocations with an ack.** `/backfill orders
  2024-01-01` is `func` + `args` with the arguments already parsed out. The
  command declaration is very nearly the function signature.
- Also available as triggers: message shortcuts (kick a workflow off *from* a
  message), @-mentions, emoji reactions on a message.
- **Socket Mode is a websocket dialed outbound.** No public URL, no tunnel,
  works behind NAT on a laptop. Bot token plus app token and events arrive.
- Cost: app manifests and OAuth scopes are the fiddliest onboarding here, and a
  workspace admin may have to approve the install.

### Discord — the shortest path

Where this project's users already are; the README links the invite. Bot-shaped
and developer-shaped, and the Gateway is again an outbound websocket.

- **Cheapest onboarding of anything on this list.** Create an app, invite the
  bot, paste one token. No workspace admin, no business verification, no
  billing, no tunnel.
- Application commands are *typed*, where Slack's `text` is one raw string.
  Under the pass-through rule this stops mattering — both hand the user's
  content to the worker as it arrived — so it is no longer an argument for
  building Discord first.
- Cost: the Gateway is more protocol than Socket Mode (heartbeats, resume with
  a session id, eventually sharding), and less enterprise pull. Its users are
  building bots and agents rather than ops tooling.

### Why not WhatsApp

The largest raw audience and the worst first run: a Meta Business account, a
verified phone number, a publicly reachable HTTPS callback, and a 24-hour
messaging window outside which only pre-approved templates may be sent. Nobody
evaluates it on a Tuesday afternoon. Good second year, bad first trigger.

### The queues are not on this list

Kafka and NATS are a separate axis, not a third chat platform, and comparing
them here would be comparing the wrong things. A chat trigger is judged on how
fast someone can try it; a queue trigger is judged on whether its offset
semantics are right, and it carries the filter and transform that the chat
mapping deliberately does without. They share the crate shape and the
`promise.create` at the end of it, and nothing else. See the queue section
above.

## Recommendation

**Slack first, Discord second.** Everything between "an event arrived" and "a
promise was created" is shared; what differs is the wire — socket handshake,
payload shape, and which fields the id template can draw on.

The gap is closer than it looks, though: Discord ships in a fraction of the
time and lands in front of the people already in your Discord. If the goal is
feedback within a week rather than reach within a quarter, invert the order.

```
crates/resonate-trigger-slack/
  src/lib.rs      Config + the ResonateGateway impl: connect, hold, drain.
  src/invoke.rs   Slack event -> promise id, param, tags. The whole
                  provider-specific surface, and the only part Discord
                  would not reuse.
```

Named `trigger-`, not `gateway-`, because it does not speak the Resonate
protocol. It speaks Slack's, and turns it into one operation.

## One alternative worth naming

Scoped to kickoff only, the provider-specific surface is thin: verify a
signature, pull a few fields, fill a template. That is small enough that a
single `resonate-trigger-webhook` crate — an endpoint, a per-source signature
scheme, and the config block above — would cover Slack, Discord, GitHub, Stripe
and Linear at once, with Slack as the first configured source.

The reason not to lead with it: a generic webhook trigger needs a public URL,
and the outbound websocket is exactly what makes Slack and Discord runnable on
a laptop in five minutes. Reach is worth less than a demo that works on the
first try. Worth building second, once one concrete source has shown what the
generic shape has to hold.
