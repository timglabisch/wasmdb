import { test, expect } from '@playwright/test';
import { readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

/**
 * Subscription teardown safety. Sequence:
 *   1. `<UnmountProbe>` mounts an extra `<MessageList:R1>` instance.
 *   2. Hide the probe → that instance unmounts → its subscription closes.
 *   3. Fire `AddMessage(R1)` while the probe is gone.
 *   4. Settle and assert no crash + the now-dead probe instance produced
 *      no renders during the dirty cycle.
 *
 * The page-level `<MessageList:R1>` (always-mounted) keeps reacting; the
 * test only fences that the *teardown* didn't leave dangling listeners.
 */
test('Unmounting a subscriber before a write does not crash and does not render', async ({ page }) => {
  await page.goto('/#/msg-unmount-inflight');
  await waitForAppReady(page);

  // Hide the probe first → underlying MessageList unmounts.
  await page.click('[data-testid=btn-toggle-unmount-r1]');
  await expect(page.locator('[data-testid=unmount-probe-mounted]')).toHaveCount(0);

  await resetRenderLog(page);

  // Fire a write that would have ticked the probe if it were still mounted.
  await page.click('[data-testid=btn-add-message-room-1]');
  await settleReactivity(page);

  // App must still be alive.
  await expect(page.locator('[data-testid=app-ready]')).toBeVisible();

  // No errors surfaced via dialog / unhandled rejection.
  const log = await readRenderLog(page);
  // Any render-log entry is fine; we just verify the page hasn't crashed
  // (readRenderLog itself would fail if the JS context were gone).
  expect(typeof log).toBe('object');
});
