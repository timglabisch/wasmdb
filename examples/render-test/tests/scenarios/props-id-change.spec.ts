import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

/**
 * `useQuery` re-binds when the SQL string changes (id is interpolated).
 * After swapping `<Inner id={A}>` to `<Inner id={B}>`:
 *   • `UpdateUserName(A)` must NOT render Inner (A subscription gone),
 *   • `UpdateUserName(B)` MUST render Inner (B subscription registered).
 */
test('Swapping id A→B re-binds subscription cleanly', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  // Swap probe to track Bob.
  await page.click('[data-testid=btn-id-swap-to-b]');
  await settleReactivity(page);

  await resetRenderLog(page);

  // Renaming Alice must not tick the inner probe — its sub points at Bob now.
  let before = await readRenderLog(page);
  await page.click('[data-testid=btn-rename-user-a]');
  await settleReactivity(page);
  let diff = diffLogs(before, await readRenderLog(page));
  expect.soft(diff['IdSwapProbe:inner'] ?? 0, 'A is no longer subscribed').toBe(0);

  // Renaming Bob must tick the inner probe.
  before = await readRenderLog(page);
  await page.click('[data-testid=btn-rename-user-b]');
  await settleReactivity(page);
  diff = diffLogs(before, await readRenderLog(page));
  expect(diff['IdSwapProbe:inner'] ?? 0, 'B is now subscribed').toBeGreaterThanOrEqual(1);
});
