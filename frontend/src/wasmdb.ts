import { useEffect, useState } from "react";
import { z } from "zod";
import init, {
  sync as wasmSync,
  reset as wasmReset,
  register_projection as wasmRegisterProjection,
  unregister_projection as wasmUnregisterProjection,
  register_table as wasmRegisterTable,
  ts_to_rust_ptr,
  ts_to_rust_len,
  flush_ts_buffer,
} from "wasm-lib";

const wasm = await init();

// --- Table ---

export class Table<T extends Record<string, string>> {
  readonly fieldNames: string[];
  readonly tableId: number;
  readonly fixedSize: number; // pre-computed: table_id + id_len prefix + val_len prefixes

  constructor(
    public readonly name: string,
    public readonly schema: z.ZodType<T>,
  ) {
    const shape = (schema as any).shape;
    this.fieldNames =
      shape && typeof shape === "object" ? Object.keys(shape) : [];
    this.tableId = wasmRegisterTable(name, this.fieldNames);
    // 2(table_id) + 2(id_len) + 2*numFields(val_len prefixes)
    this.fixedSize = 2 + 2 + 2 * this.fieldNames.length;
  }
}

// --- Query DSL (typed per table) ---

type WithId<T> = T & { _id: string };

interface TermQuery<T> {
  term: Partial<WithId<T>>;
}

interface BoolQuery<T> {
  bool: {
    must?: Query<T>[];
    must_not?: Query<T>[];
  };
}

export type Query<T> = TermQuery<T> | BoolQuery<T>;

// --- Internal types ---

interface Diff {
  version: number;
  table: string;
  id: string;
  key: string;
  value: string;
  diff: number;
}

// --- Projection ---

export type ProjectionData<T> = Record<string, T>;

// --- WasmDb ---

export class WasmDb {
  private memory = wasm.memory;

  // TS→Rust shared buffer state
  private _tsBuffer: Uint8Array | null = null;
  private _tsView: DataView | null = null;
  private _lastBuffer: ArrayBufferLike | null = null;
  private writePos: number = 8;
  private readonly tsBufferSize = ts_to_rust_len();

  private get tsBuffer(): Uint8Array {
    if (this._lastBuffer !== this.memory.buffer) {
      const ptr = ts_to_rust_ptr();
      this._tsBuffer = new Uint8Array(this.memory.buffer, ptr, this.tsBufferSize);
      this._tsView = new DataView(this.memory.buffer, ptr, this.tsBufferSize);
      this._lastBuffer = this.memory.buffer;
    }
    return this._tsBuffer!;
  }

  private get tsView(): DataView {
    if (this._lastBuffer !== this.memory.buffer) {
      const ptr = ts_to_rust_ptr();
      this._tsBuffer = new Uint8Array(this.memory.buffer, ptr, this.tsBufferSize);
      this._tsView = new DataView(this.memory.buffer, ptr, this.tsBufferSize);
      this._lastBuffer = this.memory.buffer;
    }
    return this._tsView!;
  }

  add<T extends Record<string, string>>(
    table: Table<T>,
    id: string,
    data: T,
  ): void {
    // Size check: fixed parts + variable parts (id + values)
    let varLen = id.length;
    for (let i = 0; i < table.fieldNames.length; i++) {
      varLen += (data[table.fieldNames[i] as keyof T] as string)?.length ?? 0;
    }
    if (this.writePos + table.fixedSize + varLen > this.tsBufferSize) {
      this.flush();
    }

    const buf = this.tsBuffer;
    let pos = this.writePos;

    // Table ID (2 bytes)
    buf[pos] = table.tableId;
    buf[pos + 1] = table.tableId >> 8;
    pos += 2;

    // ID
    buf[pos] = id.length;
    buf[pos + 1] = id.length >> 8;
    pos += 2;
    for (let i = 0; i < id.length; i++) {
      buf[pos++] = id.charCodeAt(i);
    }

    // Values only (in schema order)
    for (let fi = 0; fi < table.fieldNames.length; fi++) {
      const val = (data[table.fieldNames[fi] as keyof T] as string) ?? "";
      buf[pos] = val.length;
      buf[pos + 1] = val.length >> 8;
      pos += 2;
      for (let i = 0; i < val.length; i++) {
        buf[pos++] = val.charCodeAt(i);
      }
    }

    this.writePos = pos;
  }

  private flush(): void {
    if (this.writePos <= 8) return;
    // Set header
    this.tsView.setUint32(0, 8, true); // from_offset
    this.tsView.setUint32(4, this.writePos, true); // to_offset
    flush_ts_buffer();
    this.writePos = 8;
    this._lastBuffer = null; // force view refresh on next access
  }

  registerProjection<
    T extends Record<string, string>,
    F extends keyof WithId<T> = keyof WithId<T>,
  >(
    config: { table: Table<T>; query: Query<T>; fields?: readonly F[] },
    onChanged: (data: ProjectionData<Pick<WithId<T>, F>>) => void,
  ): number {
    const data = {} as Record<string, Record<string, string>>;

    // Wrap query with _table filter
    const wrappedQuery = {
      bool: {
        must: [{ term: { _table: config.table.name } }, config.query],
      },
    };

    // Compute fields: user-specified or all schema fields + _id
    const fields = config.fields
      ? [...config.fields]
      : [...config.table.fieldNames, "_id"];

    const wasmConfig = { query: wrappedQuery, fields };

    const prefix = config.table.tableId + ":";
    const callback = (diffs: Diff[]) => {
      for (const d of diffs) {
        // Strip table_id prefix from composite ID: "0:1" -> "1"
        const id = d.id.startsWith(prefix) ? d.id.slice(prefix.length) : d.id;
        if (d.diff > 0) {
          data[id] ??= {};
          data[id][d.key] = d.value;
        } else {
          const row = data[id];
          if (!row) continue;
          delete row[d.key];
          if (Object.keys(row).length === 0) delete data[id];
        }
      }
      onChanged({ ...data } as ProjectionData<Pick<WithId<T>, F>>);
    };

    return wasmRegisterProjection(wasmConfig, callback);
  }

  unregisterProjection(id: number): void {
    wasmUnregisterProjection(id);
  }

  reset(): void {
    wasmReset();
    this.writePos = 8;
  }

  sync(): void {
    // Set header so Rust can read any pending buffer data
    this.tsView.setUint32(0, 8, true);
    this.tsView.setUint32(4, this.writePos, true);

    wasmSync(0);
    this.writePos = 8;
    this._lastBuffer = null; // force view refresh on next access
  }
}

// --- Singleton + Hook ---

export const db = new WasmDb();

export function useProjection<
  T extends Record<string, string>,
  F extends keyof WithId<T> = keyof WithId<T>,
>(config: {
  table: Table<T>;
  query: Query<T>;
  fields?: readonly F[];
}): ProjectionData<Pick<WithId<T>, F>> {
  const [data, setData] = useState<ProjectionData<Pick<WithId<T>, F>>>(
    {} as ProjectionData<Pick<WithId<T>, F>>,
  );

  useEffect(() => {
    const id = db.registerProjection(config, setData);
    return () => db.unregisterProjection(id);
  }, []);

  return data;
}
