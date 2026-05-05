import { defineConfig } from '@playwright/test';

/**
 * Render-test Playwright config. The dev workflow expects the user to
 * run `make render-test` (or its components) before invoking these tests
 * so the wasm + ui bundle exist; the `webServer` block boots the
 * Rust echo-server at port 3125 which serves the prebuilt UI from
 * `apps/ui/dist`.
 */
export default defineConfig({
  testDir: './scenarios',
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  use: {
    baseURL: 'http://localhost:3125',
    trace: 'on-first-retry',
  },
  webServer: {
    // `cwd` is `tests/`; resolve up to the repo root.
    command: 'cargo run -p render-test-server --bin server',
    cwd: '../../..',
    url: 'http://localhost:3125',
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
  projects: [
    { name: 'chromium', use: { browserName: 'chromium' } },
  ],
});
