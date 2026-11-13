import adapter from '@sveltejs/adapter-static';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [
    /**
     * A single-page app, prerendered to nothing and served from the Rust binary.
     *
     * `ssr: false` and `fallback: index.html` are what make every route — including
     * `/console/executions/checkout.order-8842` — resolve to the same document, which the
     * axum handler then serves for any path it has no asset for.
     *
     * `base` is baked in at build time, so the console's mount point is not
     * configurable at runtime. It is `/console` in the gateway and nowhere else.
     *
     * (SvelteKit 3 takes this here; `svelte.config.js` is no longer read.)
     */
    sveltekit({
      adapter: adapter({ fallback: 'index.html', precompress: false, strict: false }),
      paths: { base: '/console', relative: false },
      appDir: 'app',
      // A fixed name, not the default timestamp: `assets/` is committed, and a
      // build id that changes on every run would put a spurious diff in every
      // commit that touches the console. Nothing polls it — the console is
      // reloaded by reloading it.
      version: { name: 'resonate-console', pollInterval: 0 }
    })
  ],
  build: {
    // The whole console is one page; splitting it costs a round trip on an
    // air-gapped install and buys nothing.
    target: 'es2022',
    cssMinify: true
  },
  server: {
    // `npm run dev` talks to a Resonate server started separately.
    proxy: { '/console/rpc': { target: 'http://localhost:8001', changeOrigin: true } }
  }
});
