import initWasm, * as wasm from '../wasm-pkg/sync_demo_wasm';
import {
  useWasm as useWasmGeneric,
  provideWasm,
  execute as executeGeneric,
  executeOnStream as executeOnStreamGeneric,
  type Execution,
} from '@wasmdb/client';
import { setDebugWasm } from '@wasmdb/debug-toolbar';
import type { UserCommand } from './generated/UserCommand';

export type { UserCommand, Execution };

export { useQuery, useQueryConfirmed, createStream, flushStream, nextId } from '@wasmdb/client';

export function useWasm(): boolean {
  return useWasmGeneric(async () => {
    await initWasm();
    wasm.init();
    provideWasm(wasm as any);
    setDebugWasm(wasm as any);
  });
}

export function execute(cmd: UserCommand): Execution {
  return executeGeneric(cmd);
}

export function executeOnStream(streamId: number, cmd: UserCommand): Execution {
  return executeOnStreamGeneric(streamId, cmd);
}
