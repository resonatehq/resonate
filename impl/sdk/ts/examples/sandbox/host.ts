// The host side: watch prices on a few pages, each scraped in its own sandbox.
//
// The host keeps what the sandboxes must not have: the store of yesterday's
// prices, and the webhook that alerts. The browser — the part that runs
// whatever JavaScript a page sends it — runs somewhere else.
//
//   RESONATE_URL   the server [default http://localhost:8001]
//   BROWSER        the sandbox target, e.g. sandbox://tensorlake/cas-v1:<sha256>
//   SITE           where the pages are [default https://books.toscrape.com]
//   STORE          yesterday's prices [default ./prices.json]
//   SLACK_WEBHOOK  alert here, if set; otherwise print
//   PAGES          which catalogue pages [default 1,2,3]

import { readFile, writeFile } from "node:fs/promises";
import { type Context, Exponential, type Info, Resonate } from "../../src/async/index.js";
import type { Scrape, Selectors } from "./guest.js";

type Page = { url: string; selectors: Selectors };
type Change = { url: string; name: string; was: string; now: string };

const BROWSER = process.env.BROWSER ?? "sandbox://tensorlake/browser";
const SITE = process.env.SITE ?? "https://books.toscrape.com";
const STORE = process.env.STORE ?? "./prices.json";

async function priceWatch(ctx: Context, pages: Page[]) {
  // Fan out: ctx.rpc is eager, so every sandbox starts now, one per page.
  const scrapes = pages.map((p) =>
    ctx.rpc<Scrape>(
      "scrape",
      p.url,
      p.selectors,
      ctx.options({ target: BROWSER, retryPolicy: new Exponential({ maxRetries: 3 }) }),
    ),
  );

  // One bad page doesn't sink the run.
  const settled = await Promise.allSettled(scrapes);
  const ok = settled.flatMap((s) => (s.status === "fulfilled" ? [s.value] : []));

  const changes = await ctx.run(diffAgainstYesterday, ok);
  if (changes.length > 0) await ctx.run(alert, changes);
  return { scraped: ok.length, failed: settled.length - ok.length, changes: changes.length };
}

// Leaves: plain side effects, on the host.

async function diffAgainstYesterday(_info: Info, scrapes: Scrape[]): Promise<Change[]> {
  const yesterday: Record<string, Record<string, string>> = JSON.parse(await readFile(STORE, "utf8").catch(() => "{}"));
  const today: typeof yesterday = { ...yesterday };
  const changes: Change[] = [];
  for (const s of scrapes) {
    today[s.url] = Object.fromEntries(s.items.map((i) => [i.name, i.price]));
    for (const item of s.items) {
      const was = yesterday[s.url]?.[item.name];
      if (was !== undefined && was !== item.price) {
        changes.push({ url: s.url, name: item.name, was, now: item.price });
      }
    }
  }
  await writeFile(STORE, JSON.stringify(today, null, 2));
  return changes;
}

async function alert(_info: Info, changes: Change[]): Promise<void> {
  const text = changes.map((c) => `${c.name}: ${c.was} → ${c.now}`).join("\n");
  if (process.env.SLACK_WEBHOOK) {
    await fetch(process.env.SLACK_WEBHOOK, { method: "POST", body: JSON.stringify({ text }) });
  } else {
    console.log(`price changes:\n${text}`);
  }
}

const BOOKS: Selectors = { item: "article.product_pod", name: "h3 a", price: ".price_color" };
const PAGES: Page[] = (process.env.PAGES ?? "1,2,3").split(",").map((n) => ({
  url: `${SITE}/catalogue/page-${n}.html`,
  selectors: BOOKS,
}));

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001", group: "host" });
resonate.register("priceWatch", priceWatch);

if (process.argv.includes("--schedule")) {
  // Daily at 07:00, for as long as this worker runs.
  await resonate.schedule("price-watch", "0 7 * * *", "priceWatch", PAGES);
  console.log("scheduled price-watch daily at 07:00");
} else {
  // Once, now.
  const handle = await resonate.run(`price-watch-${Date.now()}`, priceWatch, PAGES);
  console.log(await handle.result());
  await resonate.stop();
}
