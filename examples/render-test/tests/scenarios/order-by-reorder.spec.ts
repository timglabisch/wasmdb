import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const R1 = '00000000-0000-0000-0000-000000000011';
const R2 = '00000000-0000-0000-0000-000000000022';
const R3 = '00000000-0000-0000-0000-000000000033';

/**
 * Renaming Lobby ("Lobby" → "Aaa Lobby") changes the ORDER BY position
 * in `<RoomList>`'s underlying query. The list itself must re-render
 * (rooms set membership changed in ordering); only `<RoomRow:R1>` —
 * the row whose data changed — re-renders. R2/R3 stay quiet.
 */
test('RenameRoom(R1, "Aaa…") re-orders list, only R1 row renders', async ({ page }) => {
  await page.goto('/#/room-reorder');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-rename-room-1-to-aaa]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  expect(diff[`RoomRow:${R1}`] ?? 0).toBeGreaterThanOrEqual(1);
  expect.soft(diff[`RoomRow:${R2}`] ?? 0, 'R2 must not render').toBe(0);
  expect.soft(diff[`RoomRow:${R3}`] ?? 0, 'R3 must not render').toBe(0);
  expect(diff['RoomList'] ?? 0, 'RoomList must observe the reorder').toBeGreaterThanOrEqual(1);
});
