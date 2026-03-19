import init, {
  add as wasmAdd,
  sync as wasmSync,
  rust_to_ts_ptr,
  rust_to_ts_len,
} from "wasm-lib";

type Row = Record<string, string>;
type Table = Record<string, Row>;
type Tables = Record<string, Table>;

interface Diff {
  version: number;
  table: string;
  id: string;
  key: string;
  value: string;
  diff: number; // +1 or -1
}

export class WasmDb {
  private memory: WebAssembly.Memory;
  private version = 1;
  private tables: Tables = {};

  private constructor(memory: WebAssembly.Memory) {
    this.memory = memory;
  }

  static async init(): Promise<WasmDb> {
    const wasm = await init();
    return new WasmDb(wasm.memory);
  }

  add(table: string, id: string, data: Row): void {
    wasmAdd(table, id, data);
  }

  sync(): Tables {
    this.version = wasmSync(this.version);
    const diffs = this.readDiffs();
    this.applyDiffs(diffs);
    return this.tables;
  }

  private applyDiffs(diffs: Diff[]): void {
    for (const d of diffs) {
      if (d.diff > 0) {
        this.tables[d.table] ??= {};
        this.tables[d.table][d.id] ??= {};
        this.tables[d.table][d.id][d.key] = d.value;
      } else {
        const table = this.tables[d.table];
        if (!table) continue;
        const row = table[d.id];
        if (!row) continue;
        delete row[d.key];
        if (Object.keys(row).length === 0) delete table[d.id];
        if (Object.keys(table).length === 0) delete this.tables[d.table];
      }
    }
  }

  private readDiffs(): Diff[] {
    const buf = new Uint8Array(
      this.memory.buffer,
      rust_to_ts_ptr(),
      rust_to_ts_len(),
    );
    const len =
      buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24);
    if (len === 0) return [];
    const json = new TextDecoder().decode(buf.slice(4, 4 + len));
    return JSON.parse(json);
  }
}
