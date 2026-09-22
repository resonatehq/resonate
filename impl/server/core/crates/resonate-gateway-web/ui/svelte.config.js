import adapter from '@sveltejs/adapter-static';

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
 * @type {import('@sveltejs/kit').Config}
 */
export default {
  kit: {
    adapter: adapter({ fallback: 'index.html', precompress: false, strict: false }),
    paths: { base: '/console', relative: false },
    appDir: 'app',
    // A fixed name, not the default timestamp: `assets/` is committed, and a
    // build id that changes on every run would put a spurious diff in every
    // commit that touches the console. Nothing polls it — the console is
    // reloaded by reloading it.
    version: { name: 'resonate-console', pollInterval: 0 }
  }
};
