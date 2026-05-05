import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const ALICE = '00000000-0000-0000-0000-0000000000aa';
const BOB = '00000000-0000-0000-0000-0000000000bb';
const CAROL = '00000000-0000-0000-0000-0000000000cc';

/**
 * Same user appears in multiple component instances (room owner *and*
 * message author). Updating Alice's row must fan out to *all* her
 * `<UserBadge>` instances, regardless of context tag. Closes a gap left
 * by cross-table-join, which only checks "at least one" Alice badge.
 *
 * Seed has Alice as owner of R1 + R3 (`@room:R1`, `@room:R3`) and author
 * of M1 (`@msg:M1`).
 */
test('UpdateUserName(A) re-renders every UserBadge:A instance', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-rename-user-a]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  // Every Alice instance — owner badges + author badge — must have rendered.
  const expected = [
    `UserBadge:${ALICE}@room:00000000-0000-0000-0000-000000000011`,
    `UserBadge:${ALICE}@room:00000000-0000-0000-0000-000000000033`,
    `UserBadge:${ALICE}@msg:00000000-0000-0000-0000-000000000fa1`,
  ];
  for (const tag of expected) {
    expect(diff[tag] ?? 0, `${tag} should render`).toBeGreaterThanOrEqual(1);
  }

  // Other users' badges must stay quiet.
  for (const k of Object.keys(diff)) {
    if (k.startsWith(`UserBadge:${BOB}`) || k.startsWith(`UserBadge:${CAROL}`)) {
      expect.soft(diff[k], `${k} unrelated user`).toBe(0);
    }
  }
});
