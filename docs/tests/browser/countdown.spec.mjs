import { expect, test } from '@playwright/test';

test('a keyboard user can stop, resume and reset the same countdown', async ({ page }) => {
  await page.goto('./');
  const demo = page.getByRole('region', { name: 'Countdown illustration' });
  const execution = demo.getByText('Execution / countdown.1', { exact: true });
  const heading = demo.getByRole('heading');
  const log = demo.locator('pre:visible');
  const action = demo.getByRole('button');

  async function expectStage(status, label, ticks) {
    await expect(execution).toBeVisible();
    await expect(heading).toHaveText(status);
    await expect(action).toHaveAccessibleName(label);
    await expect(log).toBeVisible();
    // Inspect the rendered history: a repeated 5 after restart must fail.
    expect((await log.innerText()).match(/^Countdown: \d+$/gm)).toEqual(ticks);
  }

  await expectStage('Running', 'Stop worker', ['Countdown: 5']);
  const initialLog = await log.innerText();

  // Start at document entry, not with focus() or a click on the button.
  for (let i = 0; i < 40 && !await action.evaluate(el => el.matches(':focus')); i++) {
    await page.keyboard.press('Tab');
  }
  await expect(action).toBeFocused();
  await page.keyboard.press('Enter');
  await expectStage('Worker offline', 'Restart worker', ['Countdown: 5']);

  await expect(action).toBeFocused();
  await page.keyboard.press('Space');
  await expectStage('Resumed', 'Try again', ['Countdown: 5', 'Countdown: 4']);

  await expect(action).toBeFocused();
  await page.keyboard.press('Enter');
  await expectStage('Running', 'Stop worker', ['Countdown: 5']);
  await expect(log).toHaveText(initialLog);
});
