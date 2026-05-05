import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const R1 = '00000000-0000-0000-0000-000000000011';
const R2 = '00000000-0000-0000-0000-000000000022';
const R3 = '00000000-0000-0000-0000-000000000033';

test('TransferRoom(R1, B) re-renders RoomRow:R1, leaves R2/R3 quiet', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-transfer-room-1-to-b]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  expect(diff[`RoomRow:${R1}`] ?? 0).toBeGreaterThanOrEqual(1);
  expect.soft(diff[`RoomRow:${R2}`] ?? 0, 'R2 must not render').toBe(0);
  expect.soft(diff[`RoomRow:${R3}`] ?? 0, 'R3 must not render').toBe(0);
});
