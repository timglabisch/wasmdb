import { createRoot } from 'react-dom/client';
import initWasm, * as wasm from 'invoice-demo-wasm';
import { provideWasm, markReady } from '@wasmdb/client';
import { setDebugWasm } from '@wasmdb/debug-toolbar';
import App from './App.tsx';
import './index.css';

void (async () => {
  await initWasm();
  wasm.init();
  provideWasm(wasm as any);
  setDebugWasm(wasm as any);
  markReady();
})();

createRoot(document.getElementById('root')!).render(<App />);
