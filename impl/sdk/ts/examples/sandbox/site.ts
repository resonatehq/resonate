// A stand-in for books.toscrape.com, for running the example offline.
//
// Same markup, but the products are rendered by JavaScript after load — a
// plain fetch sees an empty page, so scraping it takes a browser. Prices move
// with DAY, so two runs on different days have something to report.
//
//   npx tsx site.ts            # port 8099, DAY=1
//   DAY=2 npx tsx site.ts

import http from "node:http";

const PORT = Number(process.env.SITE_PORT ?? 8099);
const DAY = Number(process.env.DAY ?? 1);

const TITLES = [
  "A Light in the Attic",
  "Tipping the Velvet",
  "Soumission",
  "Sharp Objects",
  "Sapiens",
  "The Requiem Red",
  "The Dirty Little Secrets",
  "The Coming Woman",
  "The Boys in the Boat",
  "The Black Maria",
  "Starving Hearts",
  "Shakespeare's Sonnets",
];

function books(page: number) {
  return TITLES.slice((page - 1) * 4, page * 4).map((title, i) => {
    // A price per book, with one book a page moving each day.
    const base = 10 + ((page * 7 + i * 13) % 40);
    const drift = i === DAY % 4 ? DAY : 0;
    return { title, price: `£${(base + drift).toFixed(2)}` };
  });
}

http
  .createServer((req, res) => {
    const m = req.url?.match(/^\/catalogue\/page-(\d+)\.html$/);
    if (!m) {
      res.writeHead(404).end();
      return;
    }
    const data = JSON.stringify(books(Number(m[1])));
    res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    res.end(`<!doctype html><html><body><ol class="row"></ol>
<script>
  // Rendered late, like a real storefront.
  setTimeout(() => {
    document.querySelector("ol").innerHTML = ${data}.map((b) =>
      '<li><article class="product_pod"><h3><a title="' + b.title + '">' + b.title.slice(0, 12) +
      '…</a></h3><p class="price_color">' + b.price + '</p></article></li>').join("");
  }, 200);
</script></body></html>`);
  })
  .listen(PORT, "127.0.0.1", () => console.log(`site on http://127.0.0.1:${PORT} (day ${DAY})`));
