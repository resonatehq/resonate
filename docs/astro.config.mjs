import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://resonatehq.github.io',
  base: '/resonate',
  trailingSlash: 'always',
  integrations: [starlight({
    title: 'Resonate',
    description: 'Durable execution with distributed async await.',
    favicon: '/favicon.svg',
    social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/resonatehq/resonate' }],
    sidebar: [{ label: 'Get started', items: ['docs/get-started/quickstart'] }],
    customCss: ['./src/styles/site.css'],
    credits: false,
  })],
});
