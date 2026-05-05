import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const R1 = '00000000-0000-0000-0000-000000000011';
const R2 = '00000000-0000-0000-0000-000000000022';
const R3 = '00000000-0000-0000-0000-000000000033';

test('AddMessage(R1) re-renders MessageList:R1, leaves R2/R3 quiet', async ({ page }) => {
  await page.goto('/#/msg-list-membership');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-add-message-room-1]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  expect(diff[`MessageList:${R1}`] ?? 0).toBeGreaterThanOrEqual(1);
  expect.soft(diff[`MessageList:${R2}`] ?? 0, 'R2 list must not render').toBe(0);
  expect.soft(diff[`MessageList:${R3}`] ?? 0, 'R3 list must not render').toBe(0);

  // Existing seed MessageItem rows (M1/M2/M3) didn't change → must not
  // re-render. The freshly inserted message has its own MessageItem
  // component which mounts and renders (expected, not under test here).
  const SEED_MSG_IDS = [
    '00000000-0000-0000-0000-000000000fa1',
    '00000000-0000-0000-0000-000000000fa2',
    '00000000-0000-0000-0000-000000000fa3',
  ];
  for (const id of SEED_MSG_IDS) {
    expect.soft(diff[`MessageItem:${id}`] ?? 0, `${id} must not render`).toBe(0);
  }
});
