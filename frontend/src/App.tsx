import { useEffect, useState } from "react";
import init, {
  buffer_ptr,
  buffer_len,
  read_first_byte_from_rust,
} from "wasm-lib";

export function App() {
  const [rustByte, setRustByte] = useState<number | null>(null);
  const [tsByte, setTsByte] = useState<number | null>(null);

  useEffect(() => {
    init().then((wasm) => {
      // Rust reads the first byte
      const fromRust = read_first_byte_from_rust();
      setRustByte(fromRust);

      // TypeScript reads the same first byte via shared memory
      const ptr = buffer_ptr();
      const len = buffer_len();
      const view = new Uint8Array(wasm.memory.buffer, ptr, len);
      const fromTs = view[0];
      setTsByte(fromTs);
    });
  }, []);

  return (
    <div style={{ fontFamily: "monospace", padding: 32 }}>
      <h1>wasmdb - shared buffer</h1>
      <p>
        <strong>Rust</strong> reads first byte: <code>{rustByte ?? "..."}</code>
      </p>
      <p>
        <strong>TypeScript</strong> reads first byte (shared memory):{" "}
        <code>{tsByte ?? "..."}</code>
      </p>
    </div>
  );
}
