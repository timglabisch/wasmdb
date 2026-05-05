import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const C1 = '00000000-0000-0000-0000-0000000000c1';

/**
 * Regression-fence on render count. Echo-server flow per command:
 *   1. optimistic apply (1 render)
 *   2. confirmed echo applies the *same* delta again (1 render)
 * → exactly 2 renders for one `SetCounterValue`. If this drops to 1 or
 * climbs to 3+ the reactivity pump or echo-apply changed.
 */
test('SetCounterValue(C1) renders Counter:C1 exactly twice', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-increment-counter-1]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  expect(diff[`Counter:${C1}`], 'optimistic + confirmed = 2 renders').toBe(2);
});
