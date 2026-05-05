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
      ref: 'users' | 'rooms';
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
  | (NewFieldBase & { kind: 'fk'; ref: 'users' | 'rooms' });

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
    fire: (values: Record<string, unknown>) => void;
  };
}
