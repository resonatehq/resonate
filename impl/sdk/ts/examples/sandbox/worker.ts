// The worker: fans out, one sandbox per page.

import { type Context, Resonate } from "../../src/async/index.js";
import type { Scrape } from "./scraper.js";

const BROWSER = process.env.BROWSER ?? "sandbox://tensorlake/resonate-browser";
const SITE = process.env.SITE ?? "https://books.toscrape.com";

const resonate = new Resonate();

resonate.register("scrapeAll", async (ctx: Context, urls: string[]) => {
  return await Promise.all(urls.map((url) => ctx.rpc<Scrape>("scrape", url, ctx.options({ target: BROWSER }))));
});

const urls = [1, 2, 3].map((n) => `${SITE}/catalogue/page-${n}.html`);
const handle = await resonate.run(`scrape-${Date.now()}`, "scrapeAll", urls);
for (const { url, items } of (await handle.result()) as Scrape[]) {
  console.log(`${url}: ${items.length} books, first: ${items[0]?.name} ${items[0]?.price}`);
}
await resonate.stop();
