# Price watch in sandboxes

The host orchestrates; each page is scraped by a real browser in a sandbox of
its own.

```ts
async function priceWatch(ctx: Context, pages: Page[]) {
  const scrapes = pages.map((p) =>
    ctx.rpc<Scrape>("scrape", p.url, p.selectors,
      ctx.options({ target: BROWSER, retryPolicy: new Exponential({ maxRetries: 3 }) })));
  const settled = await Promise.allSettled(scrapes);
  ...
}
```

| File | Runs | Holds |
|---|---|---|
| `host.ts` | on the host | the store of yesterday's prices, the alert webhook |
| `guest.ts` | in a sandbox, behind rn8 | nothing: a browser, and the page it was sent to |
| `serve.ts` | in a sandbox | takes rn8's push and runs it through the async engine |
| `site.ts` | anywhere | a stand-in for books.toscrape.com, rendered by JavaScript |

`ctx.rpc` with a `sandbox://` target is all that sends work to a sandbox. The
browser — the part that runs whatever JavaScript a page sends it — lands in a
throwaway VM that holds no secrets and is destroyed when the step ends. A
page that fails retries in its own sandbox and, if it keeps failing, is
counted as failed without stopping the others.

## Run it locally

The `local` provider runs the guest as a host process — no isolation, but the
whole path: the server dispatches to `sandbox://local/browser`, the plugin
starts rn8, rn8 starts the guest and relays every request it makes.

```shell
# the server, with the sandbox plugin on the local provider
cargo build --manifest-path impl/server/core/Cargo.toml --bin resonate --bin rn8
resonate dev \
  --set workers.worker_sandbox.enabled=true \
  --set workers.worker_sandbox.backend=local \
  --set workers.worker_sandbox.egress=all \
  --set workers.worker_sandbox.require_digest=false \
  --set 'workers.worker_sandbox.command=["…/target/debug/rn8", "--worker-port", "0", "--", "…/node_modules/.bin/tsx", "…/examples/sandbox/guest.ts"]'

# the pages, and the host
cd impl/sdk/ts/examples/sandbox && npm install
DAY=1 npx tsx site.ts &
BROWSER=sandbox://local/browser SITE=http://127.0.0.1:8099 npx tsx host.ts
# { scraped: 3, failed: 0, changes: 0 }

# a day later, prices have moved
DAY=2 npx tsx site.ts &
BROWSER=sandbox://local/browser SITE=http://127.0.0.1:8099 npx tsx host.ts
# price changes: …
# { scraped: 3, failed: 0, changes: 6 }
```

`--worker-port 0` lets several guests share the host; in a real sandbox each
has its own network and the default port is fine.

## Run it in Tensorlake

Verified end to end: three pages of the real books.toscrape.com, each scraped
by Chromium in a Tensorlake sandbox, in about 13 seconds.

**1. Build the image into Tensorlake.** Tensorlake builds the Dockerfile on its
side and registers it under a name — no local Docker, no registry. The build
context is `Dockerfile` (this directory's, with paths flattened), a static or
glibc-compatible `rn8`, the SDK's `package.json`, `package-lock.json` and
`src/`, and this example's `guest.ts`, `serve.ts`, `package.json`,
`package-lock.json`.

```python
from tensorlake.image.sandbox_builder import build_sandbox_image

build_sandbox_image(
    "Dockerfile",
    registered_name="resonate-browser",
    # Within a free project's per-sandbox limits: 1 vCPU, 1 GiB, 10 GiB disk.
    cpus=1.0, memory_mb=1024, disk_mb=6144, builder_disk_mb=10240,
    verbose=True,
)
```

**2. Point the plugin at it.** Tensorlake does not report a registered image's
`ENTRYPOINT`, so the image rule names the command; the allow-list gives the
browser the site it scrapes and nothing else.

```toml
[workers.worker_sandbox]
enabled = true
backend = "tensorlake"
require_digest = false     # a registered name, not an OCI digest
cpus = 1
memory_mib = 1024
concurrency = 1            # a project quota of one sandbox at a time

[[workers.worker_sandbox.images]]
image = "resonate-browser"
egress = { allow = ["books.toscrape.com"] }
command = ["rn8", "--", "/sdk/node_modules/.bin/tsx", "/sdk/examples/sandbox/guest.ts"]
```

With `TENSORLAKE_API_KEY` in the server's environment.

**3. Run the host.**

```shell
BROWSER=sandbox://tensorlake/resonate-browser SITE=https://books.toscrape.com npx tsx host.ts
# { scraped: 3, failed: 0, changes: 0 }
npx tsx host.ts --schedule   # daily at 07:00
```

With a quota smaller than the fan-out, the sandboxes take turns: the plugin's
`concurrency` queues dispatches, and a create refused for quota waits and
retries rather than failing.
