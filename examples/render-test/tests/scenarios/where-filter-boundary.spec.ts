import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

/**
 * Boundary-crossing reactivity: `<OnlineUserList>` filters
 * `WHERE status = 'online'`. When Alice transitions online → busy, she
 * must drop out of the result set, so the list re-renders. The
 * predicate-changed user (Alice) is not part of the per-row component
 * graph here, so we simply assert that:
 *   • `OnlineUserList` re-renders (membership changed),
 *   • `OnlineUserList` re-renders again when Carol comes online
 *     (membership grew back).
 */
test('Online filter: status changes that cross the predicate re-render the list', async ({ page }) => {
  await page.goto('/#/user-online-filter');
  await waitForAppReady(page);

  await resetRenderLog(page);

  // Alice goes busy: leaves the filter.
  let before = await readRenderLog(page);
  await page.click('[data-testid=btn-status-user-a-busy]');
  await settleReactivity(page);
  let diff = diffLogs(before, await readRenderLog(page));
  expect(diff['OnlineUserList'] ?? 0, 'list must observe the leave').toBeGreaterThanOrEqual(1);

  // Carol comes online: enters the filter.
  before = await readRenderLog(page);
  await page.click('[data-testid=btn-status-user-c-online]');
  await settleReactivity(page);
  diff = diffLogs(before, await readRenderLog(page));
  expect(diff['OnlineUserList'] ?? 0, 'list must observe the join').toBeGreaterThanOrEqual(1);
});
