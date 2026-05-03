import initWasm, * as wasm from 'invoice-demo-wasm';
import { createWasmApp } from '@wasmdb/client';
import { setDebugWasm } from '@wasmdb/debug-toolbar';
import type { InvoiceCommand } from './generated/InvoiceCommand';

export type { InvoiceCommand };
export type { Execution, QueryParams, RequirementSpec, RequirementState, QueryWithRequires, UseQueryResult } from '@wasmdb/client';
export { useQuery, useQueryConfirmed, useAsyncQuery, useRequirements, createStream, flushStream, nextId } from '@wasmdb/client';
export { requirements } from './generated/requirements';

export const { useWasm, execute, executeOnStream, peekQuery, peekQueryAsync } = createWasmApp<InvoiceCommand>(
  initWasm,
  wasm as any,
  { onReady: (m) => setDebugWasm(m as any) },
);
