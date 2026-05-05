import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

/**
 * `peekQuery` is a one-shot read — it must not register a reactive
 * listener. `<PeekProbe>` reads `users.name` for Alice on every render,
 * but the only way to trigger a render is the explicit "force" button
 * (state-bump). `UpdateUserName(A)` must not tick `PeekProbe`.
 */
test('peekQuery does not subscribe — UpdateUserName(A) leaves PeekProbe quiet', async ({ page }) => {
  await page.goto('/#/hook-peek-query');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-rename-user-a]');
  await settleReactivity(page);

  const diff = diffLogs(before, await readRenderLog(page));
  expect(diff['PeekProbe'] ?? 0, 'peekQuery must not subscribe').toBe(0);

  // Sanity: forcing a render still works.
  await resetRenderLog(page);
  const before2 = await readRenderLog(page);
  await page.click('[data-testid=btn-peek-probe-force]');
  await settleReactivity(page);
  const diff2 = diffLogs(before2, await readRenderLog(page));
  expect(diff2['PeekProbe'] ?? 0, 'state-bump must render').toBeGreaterThanOrEqual(1);
});
