// The scraper: runs in the sandbox, behind rn8, and handles exactly one task.

import { chromium } from "playwright-core";
import { type Context, Resonate } from "../../src/async/index.js";

export type Book = { name: string; price: string };
export type Scrape = { url: string; items: Book[] };

const resonate = new Resonate();

resonate.register("scrape", async (_ctx: Context, url: string): Promise<Scrape> => {
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    await page.goto(url, { waitUntil: "networkidle" });
    const items = await page.$$eval("article.product_pod", (books) =>
      books.map((b) => ({
        name: b.querySelector("h3 a")?.getAttribute("title") ?? "",
        price: b.querySelector(".price_color")?.textContent ?? "",
      })),
    );
    return { url, items };
  } finally {
    await browser.close();
  }
});

await resonate.handle();
