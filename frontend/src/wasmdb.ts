import { useEffect, useState } from "react";
import { z } from "zod";
import init, {
  add as wasmAdd,
  sync as wasmSync,
  register_projection as wasmRegisterProjection,
  unregister_projection as wasmUnregisterProjection,
  rust_to_ts_ptr,
  rust_to_ts_len,
} from "wasm-lib";

const wasm = await init();

// --- Table ---

export class Table<T extends Record<string, string>> {
  readonly fieldNames: string[];

  constructor(
    public readonly name: string,
    public readonly schema: z.ZodType<T>,
  ) {
    const shape = (schema as any).shape;
    this.fieldNames =
      shape && typeof shape === "object" ? Object.keys(shape) : [];
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

type Row = Record<string, string>;

// --- Projection ---

export type ProjectionData<T> = Record<string, T>;

// --- WasmDb ---

export class WasmDb {
  private memory = wasm.memory;
  private version = 1;
  private tables: Record<string, Record<string, Row>> = {};

  add<T extends Record<string, string>>(
    table: Table<T>,
    id: string,
    data: T,
  ): void {
    table.schema.parse(data);
    wasmAdd(table.name, id, data);
  }

  registerProjection<
    T extends Record<string, string>,
    F extends keyof WithId<T> = keyof WithId<T>,
  >(
    config: { table: Table<T>; query: Query<T>; fields?: readonly F[] },
    onChanged: (data: ProjectionData<Pick<WithId<T>, F>>) => void,
  ): number {
    const data = {} as Record<string, Row>;

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

    const prefix = config.table.name + ":";
    const callback = (diffs: Diff[]) => {
      for (const d of diffs) {
        // Strip table prefix from composite ID: "users:1" -> "1"
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

  sync(): Record<string, Record<string, Row>> {
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
