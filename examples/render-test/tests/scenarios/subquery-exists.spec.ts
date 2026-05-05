import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

/**
 * Membership reactivity across tables: `<RoomsWithMessages>` lists rooms
 * that have at least one message. Deleting M3 (the only message in R2)
 * removes R2 from the result set → list re-renders.
 */
test('DeleteMessage(M3) re-renders RoomsWithMessages', async ({ page }) => {
  await page.goto('/#/join-subquery-exists');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-delete-message-3]');
  await settleReactivity(page);

  const diff = diffLogs(before, await readRenderLog(page));
  expect(diff['RoomsWithMessages'] ?? 0).toBeGreaterThanOrEqual(1);
});
