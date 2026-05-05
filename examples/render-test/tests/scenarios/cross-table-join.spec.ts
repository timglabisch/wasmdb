import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const ALICE = '00000000-0000-0000-0000-0000000000aa';
const BOB = '00000000-0000-0000-0000-0000000000bb';
const CAROL = '00000000-0000-0000-0000-0000000000cc';

/**
 * Cross-table reactivity: changing a row in `users` re-renders only the
 * `<UserBadge>` instances that subscribe to that user. `<RoomRow>` does
 * NOT subscribe to `users`, so it must stay quiet.
 */
test('UpdateUserName(A) re-renders all UserBadge:A instances, not RoomRow', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-rename-user-a]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  // At least one UserBadge:A instance must have rendered.
  const aliceBadges = Object.keys(diff).filter((k) => k.startsWith(`UserBadge:${ALICE}`));
  expect(aliceBadges.length, 'no Alice badge re-rendered').toBeGreaterThan(0);

  // Bob/Carol badges must stay quiet.
  for (const k of Object.keys(diff)) {
    if (k.startsWith(`UserBadge:${BOB}`) || k.startsWith(`UserBadge:${CAROL}`)) {
      expect.soft(diff[k], `${k} unaffected user`).toBe(0);
    }
  }

  // RoomRow must not re-render — it doesn't subscribe to users.
  for (const k of Object.keys(diff)) {
    if (k.startsWith('RoomRow:')) {
      expect.soft(diff[k], `${k} must not render`).toBe(0);
    }
  }

  // Counters / messages must not re-render.
  for (const k of Object.keys(diff)) {
    if (k.startsWith('Counter:') || k.startsWith('MessageItem:')) {
      expect.soft(diff[k], `${k} unrelated`).toBe(0);
    }
  }
});
