import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const R1 = '00000000-0000-0000-0000-000000000011';
const R2 = '00000000-0000-0000-0000-000000000022';
const R3 = '00000000-0000-0000-0000-000000000033';
const M1 = '00000000-0000-0000-0000-000000000fa1';
const M2 = '00000000-0000-0000-0000-000000000fa2';

/**
 * Round-trip: bulk-insert then bulk-delete the same set. After settling,
 * the list returns to its original state. Sibling rooms never observed
 * any of the noise. Existing seed messages stayed quiet throughout.
 */
test('Bulk insert → bulk delete: lists settle, siblings + seed quiet', async ({ page }) => {
  await page.goto('/#/msg-bulk-delete');
  await waitForAppReady(page);

  await page.click('[data-testid=btn-bulk-add-20-r1]');
  await settleReactivity(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-bulk-delete-r1]');
  await settleReactivity(page);

  const diff = diffLogs(before, await readRenderLog(page));

  expect(diff[`MessageList:${R1}`] ?? 0, 'R1 list must observe deletes').toBeGreaterThanOrEqual(1);
  expect.soft(diff[`MessageList:${R2}`] ?? 0, 'R2 list must not render').toBe(0);
  expect.soft(diff[`MessageList:${R3}`] ?? 0, 'R3 list must not render').toBe(0);
  expect.soft(diff[`MessageItem:${M1}`] ?? 0, 'M1 must not render').toBe(0);
  expect.soft(diff[`MessageItem:${M2}`] ?? 0, 'M2 must not render').toBe(0);
});
