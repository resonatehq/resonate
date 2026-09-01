# resonate-gateway-web

The web console: a SvelteKit app compiled into the server binary, and the route
that answers its requests.

```
GET  /                     → 303 to the console
GET  /console, /console/   → the app shell
GET  /console/<anything>   → an embedded asset, or the shell (SPA fallback)
POST /console/rpc          → one envelope in, one envelope out
```

It is **not** a gateway in the `ResonateGateway` sense — it binds no socket and
has no lifecycle. It hands back axum routes, and `resonate-gateway-http` merges
them into the router it already serves. One port, one process, one origin: the
console is served by the server it reads, so there is no CORS to configure.

## The boundary

The console's `ui.*` requests are answered **here and nowhere else**. The worker
endpoint (`POST /`) refuses the whole namespace with a 404 that says where it
lives.

That is deliberate. `ui.*` is a read model shaped for one screen at a time; the
worker protocol is shaped for one participant acting on one promise. Keeping
them apart means an SDK cannot come to depend on a request that exists to draw a
table, and the read model can change without touching the protocol workers speak.

Three requests, all read-only, defined in `resonate-core/src/ui.rs` and
implemented by every backend (SQLite, Postgres, MySQL, and the reference oracle):

| Kind | Backs | Answered in |
| --- | --- | --- |
| `ui.executions.search` | the executions list | one indexed query, plus a count when asked |
| `ui.execution.get` | the detail view — tree, tasks and root | one query over `origin_id` |
| `ui.schedules.search` | the schedules table | one query, plus a count |

The console's two writes are ordinary worker requests, not console-shaped
aliases: **Cancel** is `promise.settle` with `rejected_canceled`, and **Invoke**
is `promise.create` with a `resonate:target` tag and a base64 `{func, args,
version}` param — the same request `resonate invoke` sends, field for field. No
`ui.*` request mutates anything.

Invoke is a departure from the handoff design, which committed to Cancel as the
product's only write; it was asked for afterwards. Note that hiding the button
would not be a security control — `POST /console/rpc` takes `promise.create`
either way, exactly as `POST /` does. Auth is the control.

## Building the app

`assets/` is the SvelteKit build, **committed**. `cargo build` alone produces the
shipping binary: no node on the build machine, no network, nothing to install.

After changing anything under `ui/`:

```shell
make console          # npm ci && npm run build, then refresh assets/
```

and commit `assets/` in the same commit as the source change. `cargo test -p
resonate-gateway-web` fails if the two drift apart in the ways that matter (the
shell missing, the base path wrong, the font gone).

For a hot-reloading loop against a server you started yourself:

```shell
resonate dev &        # or `resonate serve`
make console-dev      # vite on :5173, proxying /console/rpc to :8001
```

## The app

`ui/` is SvelteKit with `adapter-static` in SPA mode (`ssr = false`), TypeScript,
and no CSS framework — one `app.css` of custom properties and a few primitives,
then scoped component styles. Inter is self-hosted, for the same reason the
console is embedded at all.

| Path | What |
| --- | --- |
| `src/lib/api.ts` | the envelope client |
| `src/lib/tree.ts` | the tree, the indentation rule, and the timeline span |
| `src/lib/types.ts` | the `ui.*` wire types |
| `src/lib/duration.ts` | `1h30m` parsing, mirroring `parse_duration` in the CLI |
| `src/lib/components/InvokeDialog.svelte` | the Invoke form |
| `src/routes/executions/` | the list, and the detail view |
| `src/routes/schedules/` | the schedules table |
| `src/routes/settings/` | server URL, auth token, poll interval — kept in the browser |

Two rules in `tree.ts` carry the detail view and are easy to lose in a rewrite:
rows are indented by **enclosing task**, not by promise depth
(`18 + taskLevel * 18 + depth * 8`), and a bar's right edge is *now* for anything
still running.

## Authentication

The same check the worker endpoint applies, when auth is configured at all: the
console's requests carry `head.auth` and go through `resonate_auth::auth_check`.
There is no login and no session — the token is typed into Settings and kept in
that browser — so a server started with auth serves a console that works once an
operator supplies a token.

## Configuration

```toml
[console]
enabled = true        # RESONATE_CONSOLE__ENABLED
redirect_root = true  # RESONATE_CONSOLE__REDIRECT_ROOT — GET / → /console/
```

The mount point is not configurable: SvelteKit bakes `base` in at build time, so
`/console` is a constant on both sides.
