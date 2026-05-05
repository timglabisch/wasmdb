import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const R1 = '00000000-0000-0000-0000-000000000011';
const M1 = '00000000-0000-0000-0000-000000000fa1';
const M2 = '00000000-0000-0000-0000-000000000fa2';

/**
 * Insert with a `created_at` *before* existing messages. The new
 * `<MessageItem>` mounts at the head of the list, but the existing M1/M2
 * rows didn't change → their `<MessageItem>`s must NOT re-render. The
 * `<MessageList:R1>` itself re-renders (membership change).
 */
test('AddMessage(R1, early) leaves seed MessageItems quiet, list renders', async ({ page }) => {
  await page.goto('/#/msg-insert-middle');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-add-message-r1-early]');
  await settleReactivity(page);

  const diff = diffLogs(before, await readRenderLog(page));

  expect(diff[`MessageList:${R1}`] ?? 0).toBeGreaterThanOrEqual(1);
  expect.soft(diff[`MessageItem:${M1}`] ?? 0, 'M1 must not render').toBe(0);
  expect.soft(diff[`MessageItem:${M2}`] ?? 0, 'M2 must not render').toBe(0);
});
