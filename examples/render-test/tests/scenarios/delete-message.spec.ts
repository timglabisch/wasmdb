import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const R1 = '00000000-0000-0000-0000-000000000011';
const R2 = '00000000-0000-0000-0000-000000000022';
const R3 = '00000000-0000-0000-0000-000000000033';
const M2 = '00000000-0000-0000-0000-000000000fa2';
const M3 = '00000000-0000-0000-0000-000000000fa3';

/**
 * List-shrink reactivity: deleting a row from `messages` re-fires the
 * per-room list query for the affected room. Sibling rooms' lists and
 * surviving `<MessageItem>`s must stay quiet — their rows didn't change.
 */
test('DeleteMessage(M1) re-renders MessageList:R1, leaves siblings quiet', async ({ page }) => {
  await page.goto('/#/msg-delete');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-delete-message-1]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  expect(diff[`MessageList:${R1}`] ?? 0).toBeGreaterThanOrEqual(1);
  expect.soft(diff[`MessageList:${R2}`] ?? 0, 'R2 list must not render').toBe(0);
  expect.soft(diff[`MessageList:${R3}`] ?? 0, 'R3 list must not render').toBe(0);

  // Surviving MessageItems whose rows didn't change must stay quiet.
  expect.soft(diff[`MessageItem:${M2}`] ?? 0, 'M2 must not render').toBe(0);
  expect.soft(diff[`MessageItem:${M3}`] ?? 0, 'M3 must not render').toBe(0);
});
