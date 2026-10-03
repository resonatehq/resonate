// The sandbox side: one function, `scrape`, run in a browser.
//
// This process is the worker inside the browser image, behind rn8. It holds no
// credentials and keeps nothing: each scrape gets a fresh sandbox, which is
// destroyed when the step ends.

import { chromium } from "playwright-core";
import type { Context } from "../../src/async/index.js";
import { serve } from "./serve.js";

export type Selectors = { item: string; name: string; price: string };
export type Item = { name: string; price: string };
export type Scrape = { url: string; at: number; items: Item[] };

async function scrape(_ctx: Context, url: string, sel: Selectors): Promise<Scrape> {
  // CHROMIUM picks a browser binary outside an image that ships one.
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || undefined });
  try {
    const page = await browser.newPage();
    await page.goto(url, { waitUntil: "networkidle", timeout: 30_000 });
    await page.waitForSelector(sel.item, { timeout: 10_000 });

    const items = await page.$$eval(
      sel.item,
      (els, s) =>
        els.map((el) => {
          const name = el.querySelector(s.name);
          return {
            name: (name?.getAttribute("title") ?? name?.textContent ?? "").trim(),
            price: (el.querySelector(s.price)?.textContent ?? "").trim(),
          };
        }),
      sel,
    );

    console.log(`scraped ${items.length} items from ${url}`);
    return { url, at: Date.now(), items };
  } finally {
    await browser.close();
  }
}

serve({ scrape });
