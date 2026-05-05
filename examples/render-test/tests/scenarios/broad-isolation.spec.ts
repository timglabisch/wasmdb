import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const C1 = '00000000-0000-0000-0000-0000000000c1';

/**
 * Negative-space test: a write to `counters` must NOT touch any unrelated
 * component. No `UserBadge`, `RoomRow`, `MessageList`, or `MessageItem`
 * may re-render. Verifies cross-table reactivity isolation.
 */
test('SetCounterValue(C1) leaves all unrelated components quiet', async ({ page }) => {
  await page.goto('/#/counter-isolation');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-increment-counter-1]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  expect(diff[`Counter:${C1}`] ?? 0).toBeGreaterThanOrEqual(1);

  for (const k of Object.keys(diff)) {
    if (
      k.startsWith('UserBadge:') ||
      k.startsWith('RoomRow:') ||
      k.startsWith('MessageList:') ||
      k.startsWith('MessageItem:')
    ) {
      expect.soft(diff[k], `${k} must not render on counter write`).toBe(0);
    }
  }
});
