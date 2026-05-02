// Run the wasi-compiled bench inside Node. Usage:
//   node --experimental-wasi-unstable-preview1 examples/run_wasm_bench.mjs

import { readFile } from 'node:fs/promises';
import { WASI } from 'node:wasi';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const wasmPath = resolve(
  __dirname,
  '../../../target/wasm32-wasip1/release/examples/bench.wasm',
);

const wasi = new WASI({
  version: 'preview1',
  args: [],
  env: process.env,
});

const wasmBuffer = await readFile(wasmPath);
const wasmModule = await WebAssembly.compile(wasmBuffer);
const instance = await WebAssembly.instantiate(wasmModule, wasi.getImportObject());

wasi.start(instance);
