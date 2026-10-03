// A stand-in for books.toscrape.com, for running the example offline.
//
// Same markup, but rendered by JavaScript after load: a plain fetch sees an
// empty page, so scraping it takes a browser.
//
//   npx tsx site.ts    # http://127.0.0.1:8099

import http from "node:http";

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

http
  .createServer((req, res) => {
    const page = Number(req.url?.match(/^\/catalogue\/page-(\d+)\.html$/)?.[1]);
    if (!page) return void res.writeHead(404).end();
    const books = TITLES.slice((page - 1) * 4, page * 4).map((title, i) => ({
      title,
      price: `£${(10 + ((page * 7 + i * 13) % 40)).toFixed(2)}`,
    }));
    res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    res.end(`<!doctype html><ol></ol><script>
  setTimeout(() => {
    document.querySelector("ol").innerHTML = ${JSON.stringify(books)}.map((b) =>
      '<li><article class="product_pod"><h3><a title="' + b.title + '">' + b.title + '</a></h3>' +
      '<p class="price_color">' + b.price + '</p></article></li>').join("");
  }, 200);
</script>`);
  })
  .listen(8099, "127.0.0.1", () => console.log("site on http://127.0.0.1:8099"));
