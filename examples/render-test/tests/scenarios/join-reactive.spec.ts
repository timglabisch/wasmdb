import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const R1 = '00000000-0000-0000-0000-000000000011';
const R2 = '00000000-0000-0000-0000-000000000022';
const R3 = '00000000-0000-0000-0000-000000000033';

/**
 * Reactive JOIN: `<RoomWithOwnerName>` reads columns from BOTH `rooms`
 * (per-row REACTIVE on `rooms.id`) and `users` (table-wide REACTIVE on
 * `users.id` — the join side can't be predicate-narrowed at query
 * binding time without knowing the FK in advance).
 *
 *   • `RenameRoom(R2)` → only join:R2 renders (per-row narrow on rooms),
 *     R1/R3 quiet.
 *   • `UpdateUserName(A)` → ALL three joins re-fire (table-wide on users).
 *     This is the documented behavior; if the engine grows correlated
 *     REACTIVE filters this assertion can tighten.
 */
test('RenameRoom(R2) renders only join:R2', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-rename-room-2]');
  await settleReactivity(page);

  const diff = diffLogs(before, await readRenderLog(page));
  expect(diff[`RoomWithOwnerName:${R2}`] ?? 0).toBeGreaterThanOrEqual(1);
  expect.soft(diff[`RoomWithOwnerName:${R1}`] ?? 0, 'join:R1 must not render').toBe(0);
  expect.soft(diff[`RoomWithOwnerName:${R3}`] ?? 0, 'join:R3 must not render').toBe(0);
});

test('UpdateUserName(A) re-fires all joins (users is table-wide reactive)', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-rename-user-a]');
  await settleReactivity(page);

  const diff = diffLogs(before, await readRenderLog(page));
  expect(diff[`RoomWithOwnerName:${R1}`] ?? 0).toBeGreaterThanOrEqual(1);
  expect(diff[`RoomWithOwnerName:${R2}`] ?? 0).toBeGreaterThanOrEqual(1);
  expect(diff[`RoomWithOwnerName:${R3}`] ?? 0).toBeGreaterThanOrEqual(1);
});
