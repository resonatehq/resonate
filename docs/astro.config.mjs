import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: process.env.PAGES_ORIGIN || 'https://resonatehq.github.io',
  base: '/resonate',
  trailingSlash: 'always',
  integrations: [starlight({
    title: 'Resonate',
    description: 'Durable execution with distributed async await.',
    logo: { src: './src/assets/echo.png', alt: '' },
    customCss: ['./src/styles/resonate.css'],
    expressiveCode: { defaultProps: { wrap: true } },
    sidebar: [{ label: 'Get started', items: [
      { label: 'Overview', slug: '' },
      { label: 'Quickstart', slug: 'docs/get-started/quickstart' },
    ] }],
    social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/resonatehq/resonate' }],
  })],
});
