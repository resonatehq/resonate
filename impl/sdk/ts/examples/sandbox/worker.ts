// The worker: fans out, one sandbox per page.

import { type Context, Resonate } from "../../src/async/index.js";
import type { Scrape } from "./scraper.js";

const resonate = new Resonate();

resonate.register("scrapeAll", async (ctx: Context, urls: string[]) => {
  return await Promise.all(
    urls.map((url) => ctx.rpc<Scrape>("scrape", url, ctx.options({ target: "sandbox://tensorlake/resonate-browser" }))),
  );
});

await resonate.listen();
