import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const R1 = '00000000-0000-0000-0000-000000000011';
const R2 = '00000000-0000-0000-0000-000000000022';
const R3 = '00000000-0000-0000-0000-000000000033';

/**
 * Aggregate (`COUNT(*)`) over a per-room slice. The aggregate must
 * react to membership changes in *its* slice only — `AddMessage(R1)`
 * re-renders `MessageCount:R1`, leaves `R2`/`R3` counts untouched.
 */
test('AddMessage(R1) re-renders MessageCount:R1, leaves R2/R3 quiet', async ({ page }) => {
  await page.goto('/#/msg-count');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-add-message-room-1]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  expect(diff[`MessageCount:${R1}`] ?? 0).toBeGreaterThanOrEqual(1);
  expect.soft(diff[`MessageCount:${R2}`] ?? 0, 'R2 count must not render').toBe(0);
  expect.soft(diff[`MessageCount:${R3}`] ?? 0, 'R3 count must not render').toBe(0);
});
