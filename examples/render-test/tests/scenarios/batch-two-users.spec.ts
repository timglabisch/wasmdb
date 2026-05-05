import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const ALICE = '00000000-0000-0000-0000-0000000000aa';
const BOB = '00000000-0000-0000-0000-0000000000bb';
const CAROL = '00000000-0000-0000-0000-0000000000cc';

/**
 * Two `UpdateUserName` commands fired synchronously in the same tick.
 * Both Alice's and Bob's badges must render; Carol's must stay quiet.
 * Verifies multi-write isolation: a single user-action that touches
 * two rows must fan out to exactly those two subscriber sets, not more.
 */
test('Rename A + B in one tick re-renders both, leaves Carol quiet', async ({ page }) => {
  await page.goto('/#/user-batch');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-rename-users-a-and-b]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  const aliceBadges = Object.keys(diff).filter((k) => k.startsWith(`UserBadge:${ALICE}`));
  const bobBadges = Object.keys(diff).filter((k) => k.startsWith(`UserBadge:${BOB}`));
  expect(aliceBadges.length, 'no Alice badge re-rendered').toBeGreaterThan(0);
  expect(bobBadges.length, 'no Bob badge re-rendered').toBeGreaterThan(0);

  // Carol's badges must stay quiet — her row wasn't touched.
  for (const k of Object.keys(diff)) {
    if (k.startsWith(`UserBadge:${CAROL}`)) {
      expect.soft(diff[k], `${k} unrelated user`).toBe(0);
    }
  }
});
