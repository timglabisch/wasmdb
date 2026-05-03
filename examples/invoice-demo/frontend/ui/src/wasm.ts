import initWasm, * as wasm from 'invoice-demo-wasm';
import {
  useWasm as useWasmGeneric,
  provideWasm,
  execute as executeGeneric,
  executeOnStream as executeOnStreamGeneric,
  type Execution,
  type QueryParams,
} from '@wasmdb/client';
import { setDebugWasm } from '@wasmdb/debug-toolbar';
import type { InvoiceCommand } from './generated/InvoiceCommand';

export type { InvoiceCommand, Execution };

export { useQuery, useQueryConfirmed, useAsyncQuery, useRequirements, createStream, flushStream, nextId } from '@wasmdb/client';
export type {
  QueryParams,
  RequirementSpec,
  RequirementState,
  QueryWithRequires,
  UseQueryResult,
} from '@wasmdb/client';
export { requirements } from './generated/requirements';

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
export function peekQuery(sql: string, params?: QueryParams): any[][] {
  return (wasm as any).query(sql, params);
}

/**
 * Async sibling of `peekQuery`. Required when the SQL contains a
 * `schema.fn(args)` source — the sync path refuses those because the
 * fetcher needs an HTTP roundtrip. Use to one-shot trigger a fetcher
 * (and thus populate the local table) at component mount.
 */
export function peekQueryAsync(sql: string, params?: QueryParams): Promise<any[][]> {
  return (wasm as any).query_async(sql, params);
}
