import { defineConfig } from '@playwright/test';

const baseURL = 'http://127.0.0.1:4321/resonate/';

export default defineConfig({
  testDir: './tests/browser',
  use: { baseURL, channel: 'chromium' },
  webServer: {
    // Keep Astro in the foreground so the test runner owns its lifecycle.
    command: 'npm run preview -- --ignore-lock --host 127.0.0.1 --port 4321',
    url: baseURL,
    reuseExistingServer: false,
  },
});
