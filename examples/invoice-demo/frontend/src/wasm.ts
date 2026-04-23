import initWasm, * as wasm from '../wasm-pkg/invoice_demo_wasm';
import {
  useWasm as useWasmGeneric,
  provideWasm,
  execute as executeGeneric,
  executeOnStream as executeOnStreamGeneric,
  type Execution,
} from '@wasmdb/client';
import { setDebugWasm } from '@wasmdb/debug-toolbar';
import type { InvoiceCommand } from './generated/InvoiceCommand';

export type { InvoiceCommand, Execution };

export { useQuery, useQueryConfirmed, useAsyncQuery, createStream, flushStream, nextId } from '@wasmdb/client';

/** Boot the WASM module once and wire it to the reactive client. */
export function useWasm(): boolean {
  return useWasmGeneric(async () => {
    await initWasm();
    wasm.init();
    provideWasm(wasm as any);
    setDebugWasm(wasm as any);
  });
}

/** Queue a write. Fire-and-forget: returns an Execution you can await. */
export function execute(cmd: InvoiceCommand): Execution {
  return executeGeneric(cmd);
}

/** Queue a write on an explicit stream — used to atomically bundle multiple commands. */
export function executeOnStream(streamId: number, cmd: InvoiceCommand): Execution {
  return executeOnStreamGeneric(streamId, cmd);
}

/**
 * Synchronous non-reactive read. Use for one-shot lookups at write time
 * (actions, commands) where you do not want to subscribe the caller to
 * re-renders. Returns raw rows — callers map columns themselves.
 */
export function peekQuery(sql: string): any[][] {
  return (wasm as any).query(sql);
}
