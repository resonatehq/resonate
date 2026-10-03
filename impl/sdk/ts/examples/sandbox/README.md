# Scraping in sandboxes

A worker fans out; each page is scraped by a real browser in a sandbox of its
own. Both sides use the same API.

```ts
// worker.ts — on the host
const resonate = new Resonate();

resonate.register("scrapeAll", async (ctx: Context, urls: string[]) => {
  return await Promise.all(
    urls.map((url) => ctx.rpc<Scrape>("scrape", url, ctx.options({ target: "sandbox://tensorlake/resonate-browser" }))),
  );
});

await resonate.listen();
```

```ts
// scraper.ts — in the sandbox
const resonate = new Resonate();

resonate.register("scrape", async (_ctx: Context, url: string): Promise<Scrape> => {
  // open the page in Chromium, read the books
});

await resonate.handle();
```

`ctx.rpc` with a `sandbox://` target is all that sends work to a sandbox.
Receiving work is explicit on both sides: `listen()` takes tasks until
`stop()`, `handle()` takes exactly one and shuts the instance down. In the
sandbox, rn8 starts `scraper.ts` with `RESONATE_PUSH=1`, so the one task
arrives as rn8's HTTP push rather than from the server — nothing lingers. (A push worker that
should stay up between tasks, like a reused serverless container, calls
`resonate.listen()` instead: same pushes, until `stop()`. Both sit on
`resonate.fetch(request)`, a web-standard fetch handler that Deno, Bun and
Cloudflare Workers take as is.) The browser, the
part that runs whatever JavaScript a page sends it, lands in a throwaway VM
that holds no secrets.

`site.ts` is a stand-in for books.toscrape.com, rendered by JavaScript, for
running offline.

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
  --set 'workers.worker_sandbox.command=["…/target/debug/rn8", "--worker-port", "0", "--", "…/node_modules/.bin/tsx", "…/examples/sandbox/scraper.ts"]'

cd impl/sdk/ts/examples/sandbox && npm install
npx tsx site.ts &
# with the target in worker.ts set to "sandbox://local/browser"
npx tsx worker.ts &
resonate invoke scrape-1 --func scrapeAll \
  --arg '["http://127.0.0.1:8099/catalogue/page-1.html", "http://127.0.0.1:8099/catalogue/page-2.html"]'
resonate promises get scrape-1
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
`src/`, and this example's `scraper.ts`, `package.json`, `package-lock.json`.

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
command = ["rn8", "--", "/sdk/node_modules/.bin/tsx", "/sdk/examples/sandbox/scraper.ts"]
```

With `TENSORLAKE_API_KEY` in the server's environment.

**3. Run the worker, and invoke it.**

```shell
npx tsx worker.ts &
resonate invoke scrape-1 --func scrapeAll --arg '["https://books.toscrape.com/catalogue/page-1.html",
  "https://books.toscrape.com/catalogue/page-2.html", "https://books.toscrape.com/catalogue/page-3.html"]'
resonate promises get scrape-1   # resolved: three pages, 20 books each
```

With a quota smaller than the fan-out, the sandboxes take turns: the plugin's
`concurrency` queues dispatches, and a create refused for quota waits and
retries rather than failing.

## Run it in Unikraft Cloud

Verified end to end: the same three pages, each scraped by Chromium in its own
Unikraft instance, in about 8 seconds; an instance boots and runs its step in
2–4.

Unikraft gives an instance no stdin, so rn8 takes its frames over HTTP
instead (`--listen`), behind Unikraft's edge and a token only the plugin
holds. The image is the same scraper, but small: an account's images are
capped at 1 GiB, and Playwright's image alone is 2.6 GB.

**1. Build and push the image.** `unikraft/Dockerfile` copies only what the
scraper runs out of Playwright's image (~580 MB). Its build context holds a
static `rn8` and the SDK with its dependencies already installed — only
`cron-parser`, `eventsource` and `tsx` at the top level, and this example's
`playwright-core`:

```shell
cargo build --release -p resonate-sandbox-rn8 --target x86_64-unknown-linux-musl
# context/rn8, context/sdk/{src,examples/sandbox,node_modules}, then:
cp unikraft/Dockerfile unikraft/Kraftfile context/
cd context && UKC_TOKEN=… kraft pkg --push --plat kraftcloud --arch x86_64 \
  --name index.unikraft.io/<user>/resonate-browser:latest .
```

**2. Point the plugin at it.** Unikraft has no outbound-network policy, so the
provider requires `egress = "all"`; the image's own command is rn8.

```toml
[workers.worker_sandbox]
enabled = true
backend = "unikraft"
require_digest = false     # a registry tag, not an OCI digest
egress = "all"             # Unikraft cannot restrict an instance's network
cpus = 1
memory_mib = 2048
```

With `UKC_TOKEN` in the server's environment.

**3. Run the worker, and invoke it**, with the target in `worker.ts` set to
`"sandbox://unikraft/<user>/resonate-browser:latest"` — as for Tensorlake.
Wider than the account's memory quota, a fan-out queues: a create refused for
quota waits and retries.
