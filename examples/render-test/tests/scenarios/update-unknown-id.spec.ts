import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

/**
 * Updating a row that no component subscribes to must not tick anything.
 * Echo-server still performs an INSERT/UPDATE roundtrip; the dirty-cycle
 * runs but no per-row predicate matches, so no `<UserBadge>` re-renders.
 */
test('UpdateUserName(unknown UUID) leaves all UserBadges quiet', async ({ page }) => {
  await page.goto('/#/user-unknown-id');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-rename-unknown-user]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  for (const k of Object.keys(diff)) {
    if (k.startsWith('UserBadge:')) {
      expect.soft(diff[k], `${k} must not render for unknown id`).toBe(0);
    }
  }
});
