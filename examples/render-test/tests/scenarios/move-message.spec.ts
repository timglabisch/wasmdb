import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const R1 = '00000000-0000-0000-0000-000000000011';
const R2 = '00000000-0000-0000-0000-000000000022';
const R3 = '00000000-0000-0000-0000-000000000033';

/**
 * Cross-list membership move: M1 transitions from R1 to R2. Both lists
 * must observe their own membership change. R3 stays quiet. The
 * `<MessageItem:M1>` itself re-renders (its `room_id` row changed).
 */
test('MoveMessage(M1, R1→R2) re-renders both lists, R3 quiet', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-move-message-1-to-r2]');
  await settleReactivity(page);

  const diff = diffLogs(before, await readRenderLog(page));
  expect(diff[`MessageList:${R1}`] ?? 0, 'R1 list loses M1').toBeGreaterThanOrEqual(1);
  expect(diff[`MessageList:${R2}`] ?? 0, 'R2 list gains M1').toBeGreaterThanOrEqual(1);
  expect.soft(diff[`MessageList:${R3}`] ?? 0, 'R3 list must not render').toBe(0);
});
