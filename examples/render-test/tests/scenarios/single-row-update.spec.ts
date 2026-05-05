import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const C1 = '00000000-0000-0000-0000-0000000000c1';
const C2 = '00000000-0000-0000-0000-0000000000c2';
const C3 = '00000000-0000-0000-0000-0000000000c3';
const C4 = '00000000-0000-0000-0000-0000000000c4';

/**
 * Echo-server emits optimistic apply followed by an identical confirmed
 * apply, so the targeted component re-renders 1–2 times. The test only
 * asserts the *targeting*: which components rendered AT ALL — never how
 * many times. Other counters must stay strictly at 0.
 */
test('SetCounterValue(C1) re-renders only Counter:C1', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-increment-counter-1]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  expect(diff[`Counter:${C1}`] ?? 0).toBeGreaterThanOrEqual(1);
  expect.soft(diff[`Counter:${C2}`] ?? 0, 'C2 must not render').toBe(0);
  expect.soft(diff[`Counter:${C3}`] ?? 0, 'C3 must not render').toBe(0);
  expect.soft(diff[`Counter:${C4}`] ?? 0, 'C4 must not render').toBe(0);

  await expect(page.locator(`[data-testid=counter-value-${C1}]`)).toHaveText('1');
});
