import { createRoot } from 'react-dom/client';
import initWasm, * as wasm from 'render-test-wasm';
import { provideWasm, markReady } from '@wasmdb/client';
import { setDebugWasm } from '@wasmdb/debug-toolbar';
import { resetRenderLog } from '@wasmdb/scenarios';
import App from './App.tsx';

void (async () => {
  await initWasm();
  wasm.init();
  provideWasm(wasm as any);
  setDebugWasm(wasm as any);
  markReady();
})();

// Expose render-log reset to Playwright. Must be on `window` directly so
// `page.evaluate` can call it without needing a React handle.
(window as any).__resetRenderLog = resetRenderLog;

// NB: NO StrictMode — strict-mode double renders would invalidate counts.
createRoot(document.getElementById('root')!).render(<App />);
