import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const ALICE = '00000000-0000-0000-0000-0000000000aa';
const BOB = '00000000-0000-0000-0000-0000000000bb';
const CAROL = '00000000-0000-0000-0000-0000000000cc';

test('UpdateUserStatus(A) only re-renders Alice badges, leaves Bob/Carol quiet', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-status-user-a-busy]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  // At least one Alice badge instance must have re-rendered.
  const aliceBadges = Object.keys(diff).filter((k) => k.startsWith(`UserBadge:${ALICE}`));
  expect(aliceBadges.length, 'no Alice badge re-rendered').toBeGreaterThan(0);

  // Bob and Carol badges must NOT re-render.
  for (const k of Object.keys(diff)) {
    if (k.startsWith(`UserBadge:${BOB}`) || k.startsWith(`UserBadge:${CAROL}`)) {
      expect.soft(diff[k], `${k} unaffected user`).toBe(0);
    }
  }
});
