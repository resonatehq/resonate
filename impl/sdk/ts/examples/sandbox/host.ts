// The host side: scrape a few pages, each in a browser in its own sandbox.
//
//   RESONATE_URL  the server [default http://localhost:8001]
//   BROWSER       the sandbox target, e.g. sandbox://tensorlake/resonate-browser
//   SITE          where the pages are [default https://books.toscrape.com]

import { type Context, Resonate } from "../../src/async/index.js";
import type { Scrape } from "./guest.js";

const BROWSER = process.env.BROWSER ?? "sandbox://tensorlake/resonate-browser";
const SITE = process.env.SITE ?? "https://books.toscrape.com";

async function scrapeAll(ctx: Context, urls: string[]) {
  // One sandbox per page, all started at once.
  const scrapes = urls.map((url) => ctx.rpc<Scrape>("scrape", url, ctx.options({ target: BROWSER })));
  return await Promise.all(scrapes);
}

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
resonate.register("scrapeAll", scrapeAll);

const urls = [1, 2, 3].map((n) => `${SITE}/catalogue/page-${n}.html`);
const handle = await resonate.run(`scrape-${Date.now()}`, scrapeAll, urls);
for (const { url, items } of await handle.result()) {
  console.log(`${url}: ${items.length} books, first: ${items[0]?.name} ${items[0]?.price}`);
}
await resonate.stop();
