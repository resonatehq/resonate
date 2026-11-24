import { expect, test } from '@playwright/test';

test.use({ viewport: { width: 375, height: 812 } });

// One recognizable fragment per example, independent of syntax highlighting.
// Start away from the default, so selecting TypeScript is a real transition too.
const examples = [
  { sdk: 'Python', install: 'pip install resonate-sdk', fn: 'async def countdown', worker: 'python countdown.py' },
  { sdk: 'Rust', install: 'package = "resonate-sdk"', fn: 'async fn countdown', worker: 'cargo run' },
  { sdk: 'Go', install: 'go get github.com/resonatehq', fn: 'func countdown(', worker: 'go run .' },
  { sdk: 'Java', install: 'io.resonatehq:resonate-sdk-java', fn: 'class Quickstart', worker: './gradlew run' },
  { sdk: 'TypeScript', install: 'npm install @resonatehq/sdk', fn: 'function* countdown', worker: 'npx ts-node countdown.ts' },
];

test('all four quickstart sections follow each SDK selection on mobile', async ({ page }) => {
  await page.goto('docs/get-started/quickstart/');
  const groups = page.locator('main starlight-tabs');
  const sections = ['Install the SDK', 'Write a function', 'Start the worker', 'Result'];
  await expect(groups).toHaveCount(sections.length);

  for (const [index, { sdk, install, fn, worker }] of examples.entries()) {
    // Change the SDK from different sections to exercise sync in both directions.
    const source = index % sections.length;
    await test.step(`Select ${sdk} in ${sections[source]}`, async () => {
      await groups.nth(source).getByRole('tab', { name: sdk, exact: true }).click();

      for (const [i, fragment] of [install, fn, worker, worker].entries()) {
        await test.step(`Check ${sections[i]}`, async () => {
          const group = groups.nth(i);
          await expect(group.getByRole('tab', { selected: true })).toHaveText([sdk]);
          const panel = group.getByRole('tabpanel'); // Excludes hidden panels.
          await expect(panel).toBeVisible();
          await expect(panel).toHaveAccessibleName(sdk);
          await expect(panel).toContainText(fragment);
          if (sections[i] === 'Result') await expect(panel).toContainText('Countdown: 4');
        });
      }
    });
  }
});
