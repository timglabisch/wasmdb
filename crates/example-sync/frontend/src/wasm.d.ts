declare module '../wasm-pkg/example_sync_wasm' {
  export function init(): void;
  export function create_stream(): bigint;
  export function insert_user(stream_id: bigint, id: bigint, name: string, age: bigint): Uint8Array;
  export function receive_response(response_bytes: Uint8Array): string;
  export function query_users(): string;
  export default function initWasm(): Promise<unknown>;
}
