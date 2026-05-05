import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const R1 = '00000000-0000-0000-0000-000000000011';
const R2 = '00000000-0000-0000-0000-000000000022';
const R3 = '00000000-0000-0000-0000-000000000033';

/**
 * Per-row column update without FK change: only the renamed `<RoomRow>`
 * re-renders. Sibling rows stay quiet, and *no* `<UserBadge>` re-renders
 * because the owner FK didn't change. Separates "row touched" from "FK
 * changed" — the transfer-room spec covers the latter.
 */
test('RenameRoom(R2) re-renders only RoomRow:R2, all UserBadges quiet', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-rename-room-2]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  expect(diff[`RoomRow:${R2}`] ?? 0).toBeGreaterThanOrEqual(1);
  expect.soft(diff[`RoomRow:${R1}`] ?? 0, 'R1 must not render').toBe(0);
  expect.soft(diff[`RoomRow:${R3}`] ?? 0, 'R3 must not render').toBe(0);

  // No UserBadge anywhere — users untouched.
  for (const k of Object.keys(diff)) {
    if (k.startsWith('UserBadge:')) {
      expect.soft(diff[k], `${k} must not render`).toBe(0);
    }
  }
});
