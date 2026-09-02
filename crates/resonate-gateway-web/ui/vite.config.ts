import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [sveltekit()],
  build: {
    // The whole console is one page; splitting it costs a round trip on an
    // air-gapped install and buys nothing.
    target: 'es2022',
    cssMinify: true
  },
  server: {
    // `npm run dev` talks to a Resonate server started separately.
    proxy: { '/console/rpc': { target: 'http://localhost:8003', changeOrigin: true } }
  }
});
