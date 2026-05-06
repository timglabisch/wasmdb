export type Option = { value: string; label: string };

interface ColumnBase {
  key: string;
  header: string;
}

export type ColumnSpec =
  | (ColumnBase & { kind: 'id' })
  | (ColumnBase & {
      kind: 'text';
      readOnly?: boolean;
      mono?: boolean;
      onSave?: (rowId: string, value: string) => void;
    })
  | (ColumnBase & {
      kind: 'number';
      readOnly?: boolean;
      onSave?: (rowId: string, value: number) => void;
    })
  | (ColumnBase & {
      kind: 'enum';
      options: Option[];
      readOnly?: boolean;
      onSave?: (rowId: string, value: string) => void;
    })
  | (ColumnBase & {
      kind: 'fk';
      ref: string;
      readOnly?: boolean;
      onSave?: (rowId: string, value: string) => void;
    });

interface NewFieldBase {
  key: string;
  placeholder?: string;
}

export type NewFieldSpec =
  | (NewFieldBase & { kind: 'text' })
  | (NewFieldBase & { kind: 'number'; defaultValue?: number })
  | (NewFieldBase & { kind: 'enum'; options: Option[] })
  | (NewFieldBase & { kind: 'fk'; ref: string });

export interface TableSpec {
  table: string;
  label: string;
  orderBy?: string;
  columns: ColumnSpec[];
  rowAction?: {
    label: string;
    tooltip?: string;
    fire: (rowId: string) => void;
  };
  rowActionDisabledTooltip?: string;
  rowActionExtras?: (rowId: string, row: Record<string, unknown>) => React.ReactNode;
  create?: {
    label: string;
    fields: NewFieldSpec[];
    fire: (values: Record<string, unknown>, streamId?: number) => void;
  };
}

/**
 * Resolver for an `fk` column or new-row field. Each entry maps a `ref`
 * string (e.g. "users") to a SQL query that yields `[reactiveTrigger, id, label]`
 * tuples; the picker turns those into <option>s.
 *
 * The query MUST start with `SELECT REACTIVE(...)` so the dropdown stays live
 * when the referenced rows mutate.
 */
export interface FkResolver {
  /** SQL with shape `SELECT REACTIVE(...), id, label FROM ...`. */
  query: string;
  /** Build an Option from a raw row. Default picks `[1]` and `[2]`. */
  toOption?: (raw: unknown[]) => Option;
}

export interface QueryPreset {
  label: string;
  sql: string;
}

export interface LiveQueryDef {
  id: string;
  sql: string;
}

export interface PlaygroundConfig {
  specs: TableSpec[];
  /** Per-`ref` resolver. Required for any spec that uses `kind: 'fk'`. */
  fkResolvers?: Record<string, FkResolver>;
  /** Buttons shown above the custom-query editor. */
  customQueryPresets?: QueryPreset[];
  /** Initial rows of the live-queries panel. */
  liveQueries?: LiveQueryDef[];
  /** Where the sidebar "← back" link points. Defaults to `#/`. */
  backHref?: string;
  /** Label of the back link. Defaults to "← back". */
  backLabel?: string;
}
