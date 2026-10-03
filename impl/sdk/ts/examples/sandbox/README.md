# Scraping in sandboxes

The host fans out; each page is scraped by a real browser in a sandbox of its
own.

```ts
async function scrapeAll(ctx: Context, urls: string[]) {
  const scrapes = urls.map((url) => ctx.rpc<Scrape>("scrape", url, ctx.options({ target: BROWSER })));
  return await Promise.all(scrapes);
}
```

| File | Runs | |
|---|---|---|
| `host.ts` | on the host | `scrapeAll`: one `ctx.rpc` per page, to a `sandbox://` target |
| `guest.ts` | in a sandbox, behind rn8 | `scrape`: open the page in Chromium, read the books |
| `serve.ts` | in a sandbox | takes rn8's push and runs it through the async engine |
| `site.ts` | anywhere | a stand-in for books.toscrape.com, rendered by JavaScript |

`ctx.rpc` with a `sandbox://` target is all that sends work to a sandbox. The
browser — the part that runs whatever JavaScript a page sends it — lands in a
throwaway VM that holds no secrets and is destroyed when the step ends.

## Run it locally

The `local` provider runs the guest as a host process — no isolation, but the
whole path: the server dispatches to `sandbox://local/browser`, the plugin
starts rn8, rn8 starts the guest and relays every request it makes.

```shell
cargo build --manifest-path impl/server/core/Cargo.toml --bin resonate --bin rn8
resonate dev \
  --set workers.worker_sandbox.enabled=true \
  --set workers.worker_sandbox.backend=local \
  --set workers.worker_sandbox.egress=all \
  --set workers.worker_sandbox.require_digest=false \
  --set 'workers.worker_sandbox.command=["…/target/debug/rn8", "--worker-port", "0", "--", "…/node_modules/.bin/tsx", "…/examples/sandbox/guest.ts"]'

cd impl/sdk/ts/examples/sandbox && npm install
npx tsx site.ts &
BROWSER=sandbox://local/browser SITE=http://127.0.0.1:8099 npx tsx host.ts
# http://127.0.0.1:8099/catalogue/page-1.html: 4 books, first: A Light in the Attic £17.00
# …
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
BROWSER=sandbox://tensorlake/resonate-browser npx tsx host.ts
# https://books.toscrape.com/catalogue/page-1.html: 20 books, first: A Light in the Attic £51.77
# …
```

With a quota smaller than the fan-out, the sandboxes take turns: the plugin's
`concurrency` queues dispatches, and a create refused for quota waits and
retries rather than failing.
