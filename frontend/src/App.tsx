import { useEffect, useState } from "react";
import init, {
  ts_to_rust_ptr,
  ts_to_rust_len,
  rust_to_ts_ptr,
  rust_to_ts_len,
  sync,
} from "wasm-lib";

interface WasmInstance {
  memory: WebAssembly.Memory;
}

function getView(wasm: WasmInstance, ptr: number, len: number): Uint8Array {
  return new Uint8Array(wasm.memory.buffer, ptr, len);
}

export function App() {
  const [state, setState] = useState<{
    tsWrote: number;
    tsReadBack: number;
  } | null>(null);

  useEffect(() => {
    init().then((wasm) => {
      const tsToRust = getView(wasm, ts_to_rust_ptr(), ts_to_rust_len());
      const rustToTs = getView(wasm, rust_to_ts_ptr(), rust_to_ts_len());

      // TypeScript writes 0xAA into ts_to_rust
      tsToRust[0] = 0xaa;

      // Rust spiegelt ts_to_rust → rust_to_ts
      sync();

      // TypeScript liest das gespiegelte Byte aus rust_to_ts
      const tsReadsByte = rustToTs[0];

      setState({ tsWrote: 0xaa, tsReadBack: tsReadsByte });
    });
  }, []);

  return (
    <div style={{ fontFamily: "monospace", padding: 32 }}>
      <h1>wasmdb - shared buffers</h1>

      <p>TS schreibt <code>0xAA</code> → ts_to_rust[0]</p>
      <p>Rust sync: ts_to_rust → rust_to_ts</p>
      <p>
        TS liest rust_to_ts[0] zurück:{" "}
        <code>{state ? `0x${state.tsReadBack.toString(16)}` : "..."}</code>
      </p>
    </div>
  );
}
