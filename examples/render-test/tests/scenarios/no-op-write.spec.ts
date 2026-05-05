import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const ALICE = '00000000-0000-0000-0000-0000000000aa';

/**
 * Pin the no-op invariant: writing the same value still triggers a
 * dirty-cycle (engine doesn't compare old vs new). This is the *current*
 * behavior — the test fences it so a future "skip identical writes"
 * optimization is a deliberate, observable change.
 *
 * If the engine ever adds value-equality checks, flip this assertion.
 */
test('UpdateUserName(A, "Alice") with same value still re-renders Alice badges', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-rename-user-a-same]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  const aliceBadges = Object.keys(diff).filter((k) => k.startsWith(`UserBadge:${ALICE}`));
  expect(aliceBadges.length, 'no-op writes currently still fire reactivity').toBeGreaterThan(0);
});
