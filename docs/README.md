# Resonate documentation experiment

A static Astro + Starlight site for the `/resonate/` GitHub Pages project path. The two pages live together in `src/content/docs/`: `index.md` uses the built-in splash template, and `quickstart.mdx` uses the default guide layout and SDK tabs. Prefer Markdown for new pages; use MDX when a built-in component helps the reader. There are no custom components, styles, or deployment workflows.

From this directory, using Node.js 24.22.3 or later:

```sh
npm ci
npm run build
npm run preview
```

Open `/resonate/`. The quickstart keeps its original prototype URL, `/resonate/docs/get-started/quickstart/`. Guides not yet ported link to the live docs. The quickstart content and favicon come from [`resonatehq/docs.resonatehq.io` at `46596c4`](https://github.com/resonatehq/docs.resonatehq.io/tree/46596c4), under Apache-2.0.
