import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: process.env.PAGES_ORIGIN || 'https://resonatehq.github.io',
  base: '/resonate',
  trailingSlash: 'always',
  integrations: [starlight({
    title: 'Resonate',
    description: 'Durable execution with distributed async await.',
    social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/resonatehq/resonate' }],
  })],
});
