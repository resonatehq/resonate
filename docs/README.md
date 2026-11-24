# Resonate documentation

A static Astro + Starlight site for the `/resonate/` GitHub Pages project path. The two pages live together in `src/content/docs/`: `index.mdx` uses the built-in splash template, and `quickstart.mdx` uses the default guide layout and SDK tabs. Prefer Markdown for new pages; use MDX when a built-in component helps the reader. The splash includes a browser-only countdown illustration. Shared colors and typography live in `src/styles/resonate.css`.

From this directory, using Node.js 24.22.3 or later:

```sh
npm ci
npm run build
npm run check:pages
npm run preview
```

Open `/resonate/`. The quickstart is at `/resonate/docs/get-started/quickstart/`. Guides not yet ported link to the live docs. The quickstart content comes from [`resonatehq/docs.resonatehq.io` at `46596c4`](https://github.com/resonatehq/docs.resonatehq.io/tree/46596c4), under Apache-2.0.

`check:pages` reads `dist/` offline, starting at both entry points. It checks the
landing page links to the quickstart, follows same-site links, checks HTML
fragments on the destination page, and resolves local assets (including
stylesheet URLs and literal module imports). A missing file, missing fragment,
or link outside `/resonate/` fails the check and reports the referring page or
asset. External URLs and computed runtime requests are not fetched. If building
with a custom `PAGES_ORIGIN`, use the same value for the check. Run `npm run
test:pages` for the checker’s regression tests.

## Publish

Select **GitHub Actions** as the repository's Pages source, then run the **Documentation Pages** workflow from `main`. It publishes this site and the generated TypeScript SDK reference (`/resonate/api/ts/`) in one artifact. Pull requests build both but never deploy. This does not deploy or redirect `docs.resonatehq.io`.
