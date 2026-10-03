// The sandbox side: one function, `scrape`, run in a browser.
//
// This process is the worker inside the browser image, behind rn8. It holds no
// credentials and keeps nothing: each scrape gets a fresh sandbox, which is
// destroyed when the step ends.

import { chromium } from "playwright-core";
import type { Context } from "../../src/async/index.js";
import { serve } from "./serve.js";

export type Book = { name: string; price: string };
export type Scrape = { url: string; items: Book[] };

async function scrape(_ctx: Context, url: string): Promise<Scrape> {
  // CHROMIUM picks a browser binary outside an image that ships one.
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || undefined });
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
}

serve({ scrape });
