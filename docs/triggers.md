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

## Mapping an event to a workflow

The one open question — which function does an event start, with what
arguments, under what id — is the question `promiseParam` and `promiseTags`
already answer for schedules. The trigger's config is a schedule's, minus the
cron:

```toml
[[triggers.slack.commands]]
command      = "/backfill"
promise_id   = "backfill-{{.trigger_id}}"
promise_tags = { "resonate:target" = "poll://any@default" }
func         = "backfill"
args         = ["{{.text}}"]
```

Inngest answers the same question with a per-webhook JavaScript transform that
normalizes a raw payload into a typed event. That is a language, an editor and
a sandbox, for a server that otherwise decides nothing about what an operation
does. The template substitution already in the engine is the cheaper half of it
and covers the cases that matter.

Where a payload needs real reshaping, the honest default is to not reshape it:
invoke one configured function with the event passed through verbatim and let
the worker dispatch. Policy lives in the user's code, where the rest of their
policy already is.

## Idempotency is the pitch

Every provider on this list delivers at least once and will re-send on any
timeout. The usual answer is a dedupe table with a TTL. For a trigger, a
duplicate delivery is a duplicate *workflow* — the failure everyone
immediately understands.

`promise.create` is idempotent on the promise id. So a `promiseId` template
that draws on the provider's own identifier for the interaction —
`slack-{{.trigger_id}}`, `gh-{{.delivery}}` — makes at-least-once delivery into
exactly-once kickoff, with no extra state anywhere. The store the server
already has is the dedupe table, and `{{.timestamp}}` proves the templating
hook exists.

One caveat, because it is easy to get wrong: **a slash command has no
`event_id`.** That field belongs to the Events API. What a command carries is
`trigger_id`, unique per invocation, and — over Socket Mode — an `envelope_id`
on the wrapper. `trigger_id` is the better key of the two, because it names the
user's interaction rather than one delivery attempt of it. Whether Slack reuses
it across a retried delivery is the one thing here worth confirming
empirically before the pitch depends on it.

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
- Application commands are *typed* — declared options with names and types —
  so the mapping to `{func, args}` is even tighter than Slack's free-text
  `text` field.
- Cost: the Gateway is more protocol than Socket Mode (heartbeats, resume with
  a session id, eventually sharding), and less enterprise pull. Its users are
  building bots and agents rather than ops tooling.

### Why not WhatsApp, and why not Kafka first

**WhatsApp** has the largest raw audience and the worst first run: a Meta
Business account, a verified phone number, a publicly reachable HTTPS callback,
and a 24-hour messaging window outside which only pre-approved templates may be
sent. Nobody evaluates it on a Tuesday afternoon. Good second year, bad first
trigger.

**Kafka** is the most enterprise-legible and the easiest to test — a local
broker and a topic. But it reaches the fewest *new* people per unit of work,
because a team with Kafka already has a way to consume it. It is also the one
where "one record, one workflow" needs real answers about offsets, consumer
groups and partition ordering, none of which the chat platforms make us solve
on day one. Worth building. Not worth building first.

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
