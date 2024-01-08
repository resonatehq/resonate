import { jest, describe, beforeAll, afterAll } from "@jest/globals";
import { Browser, launch, Page } from "puppeteer";

let browser: Browser;
let page: Page;

jest.setTimeout(10000);

describe("Resonate Server Tests", () => {
  beforeAll(async () => {
    browser = await launch();
    // Open a new page
    [page] = await browser.pages();
    await page.goto("https://google.com");
  });

  afterAll(async () => {
    await browser.close();
  });

  require("./store.test.ts");
});
