import { memo, useMemo, useState } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount } from '../test-utils/useRenderCount';
import { useRenderFlash } from '../test-utils/useRenderFlash';
import { EditableNumber } from './EditableNumber';
import { EditableSelect } from './EditableSelect';
import { EditableText } from './EditableText';
import { RoomPicker, UserPicker } from './Pickers';
import type { ColumnSpec, NewFieldSpec, Option, TableSpec } from './types';

function Cell({
  table,
  rowId,
  col,
  value,
}: {
  table: string;
  rowId: string;
  col: ColumnSpec;
  value: unknown;
}) {
  const testid = `exp-${table}-${col.key}-${rowId}`;
  switch (col.kind) {
    case 'id':
      return (
        <span className="cell-id" title={String(value)}>
          <code>{String(value).slice(-4)}</code>
        </span>
      );
    case 'text':
      return (
        <EditableText
          value={String(value ?? '')}
          readOnly={col.readOnly}
          monospace={col.mono}
          onSave={col.onSave ? (v) => col.onSave!(rowId, v) : undefined}
          testid={testid}
        />
      );
    case 'number':
      if (col.readOnly || !col.onSave) {
        return (
          <span className="editable-display editable-readonly editable-mono">
            {Number(value ?? 0)}
          </span>
        );
      }
      return (
        <EditableNumber
          value={Number(value ?? 0)}
          onSave={(v) => col.onSave!(rowId, v)}
          testid={testid}
        />
      );
    case 'enum':
      if (col.readOnly || !col.onSave) {
        return <span className="editable-display editable-readonly">{String(value ?? '')}</span>;
      }
      return (
        <EditableSelect
          value={String(value ?? '')}
          options={col.options}
          onSave={(v) => col.onSave!(rowId, v)}
          testid={testid}
        />
      );
    case 'fk': {
      if (col.readOnly || !col.onSave) {
        return (
          <span className="cell-id" title={String(value)}>
            <code>{String(value).slice(-4)}</code>
          </span>
        );
      }
      const Picker = col.ref === 'users' ? UserPicker : RoomPicker;
      return (
        <Picker
          value={String(value ?? '')}
          onSave={(v) => col.onSave!(rowId, v)}
          testid={testid}
        />
      );
    }
  }
}

const Row = memo(function Row({ spec, rowId }: { spec: TableSpec; rowId: string }) {
  const flashRef = useRenderFlash<HTMLTableRowElement>();
  const renders = useRenderCount(`Explorer.${spec.table}.Row:${rowId}`);
  const sql = useMemo(() => {
    const cols = spec.columns.map((c) => `${spec.table}.${c.key}`).join(', ');
    return `SELECT ${cols} FROM ${spec.table} WHERE REACTIVE(${spec.table}.id = UUID '${rowId}')`;
  }, [spec, rowId]);
  const rows = useQuery<Record<string, unknown>>(sql, (raw) =>
    Object.fromEntries(spec.columns.map((c, i) => [c.key, raw[i]])),
  );
  const row = rows[0];
  if (!row) return null;
  return (
    <tr ref={flashRef} data-testid={`exp-${spec.table}-row-${rowId}`}>
      {spec.columns.map((c) => (
        <td key={c.key}>
          <Cell table={spec.table} rowId={rowId} col={c} value={row[c.key]} />
        </td>
      ))}
      <td className="cell-renders" title={`renders: ${renders}`}>r:{renders}</td>
      <td className="cell-actions">
        {spec.rowActionExtras?.(rowId, row)}
        {spec.rowAction ? (
          <button
            className="row-delete"
            data-testid={`exp-${spec.table}-action-${rowId}`}
            title={spec.rowAction.tooltip ?? spec.rowAction.label}
            onClick={() => spec.rowAction!.fire(rowId)}
          >
            {spec.rowAction.label}
          </button>
        ) : (
          <span className="row-delete-disabled" title={spec.rowActionDisabledTooltip}>×</span>
        )}
      </td>
    </tr>
  );
});

function defaultFor(field: NewFieldSpec): unknown {
  switch (field.kind) {
    case 'text': return '';
    case 'number': return field.defaultValue ?? 0;
    case 'enum': return field.options[0]?.value ?? '';
    case 'fk': return '';
  }
}

function NewFieldInput({
  table,
  field,
  value,
  onChange,
  onSubmit,
}: {
  table: string;
  field: NewFieldSpec;
  value: unknown;
  onChange: (v: unknown) => void;
  onSubmit: () => void;
}) {
  const testid = `exp-${table}-new-${field.key}`;
  if (field.kind === 'fk') {
    const Picker = field.ref === 'users' ? UserPicker : RoomPicker;
    return (
      <Picker
        value={String(value ?? '')}
        onSave={(v) => onChange(v)}
        testid={testid}
      />
    );
  }
  if (field.kind === 'enum') {
    return (
      <select
        value={String(value ?? '')}
        onChange={(e) => onChange(e.target.value)}
        data-testid={testid}
      >
        {field.options.map((o) => (
          <option key={o.value} value={o.value}>{o.label}</option>
        ))}
      </select>
    );
  }
  if (field.kind === 'number') {
    return (
      <input
        type="number"
        value={Number(value ?? 0)}
        placeholder={field.placeholder}
        onChange={(e) => onChange(Number(e.target.value))}
        onKeyDown={(e) => { if (e.key === 'Enter') onSubmit(); }}
        data-testid={testid}
      />
    );
  }
  return (
    <input
      type="text"
      value={String(value ?? '')}
      placeholder={field.placeholder}
      onChange={(e) => onChange(e.target.value)}
      onKeyDown={(e) => { if (e.key === 'Enter') onSubmit(); }}
      data-testid={testid}
    />
  );
}

function NewRowForm({ spec, fkOptions }: { spec: TableSpec; fkOptions: FkOptions }) {
  const create = spec.create;
  const initial = useMemo<Record<string, unknown>>(() => {
    if (!create) return {};
    const v: Record<string, unknown> = {};
    for (const f of create.fields) {
      v[f.key] = f.kind === 'fk'
        ? (fkOptions[f.ref]?.[0]?.value ?? '')
        : defaultFor(f);
    }
    return v;
  }, [create, fkOptions]);
  const [values, setValues] = useState<Record<string, unknown>>(initial);

  if (!create) return null;

  const valid = create.fields.every((f) => {
    const v = values[f.key];
    if (f.kind === 'text') return typeof v === 'string' && v.trim().length > 0;
    if (f.kind === 'number') return typeof v === 'number' && Number.isFinite(v);
    return Boolean(v);
  });

  const submit = () => {
    if (!valid) return;
    create.fire(values);
    setValues(initial);
  };

  return (
    <div className="explorer-new-row">
      <span className="explorer-new-label">+ {create.label}</span>
      {create.fields.map((f) => (
        <NewFieldInput
          key={f.key}
          table={spec.table}
          field={f}
          value={values[f.key] ?? defaultFor(f)}
          onChange={(v) => setValues((prev) => ({ ...prev, [f.key]: v }))}
          onSubmit={submit}
        />
      ))}
      <button
        onClick={submit}
        disabled={!valid}
        data-testid={`exp-${spec.table}-create-submit`}
      >{create.label}</button>
    </div>
  );
}

interface FkOptions {
  users?: Option[];
  rooms?: Option[];
}

export function DataTable({ spec }: { spec: TableSpec }) {
  const renders = useRenderCount(`Explorer.${spec.table}.Table`);
  const flashRef = useRenderFlash<HTMLDivElement>();
  const orderBy = spec.orderBy ?? `${spec.table}.id`;
  const ids = useQuery<{ id: string }>(
    `SELECT REACTIVE(${spec.table}.id), ${spec.table}.id FROM ${spec.table} ORDER BY ${orderBy}`,
    ([_r, id]) => ({ id: id as string }),
  );
  // FK options for the new-row form. Subscribe table-wide so the dropdowns
  // stay current. Always included so the hook order is stable.
  const userOpts = useQuery<Option>(
    'SELECT REACTIVE(users.id), users.id, users.name FROM users ORDER BY users.name',
    ([_r, id, n]) => ({ value: id as string, label: n as string }),
  );
  const roomOpts = useQuery<Option>(
    'SELECT REACTIVE(rooms.id), rooms.id, rooms.name FROM rooms ORDER BY rooms.name',
    ([_r, id, n]) => ({ value: id as string, label: n as string }),
  );
  const fkOptions: FkOptions = { users: userOpts, rooms: roomOpts };

  return (
    <section className="explorer-table">
      <header className="explorer-table-header">
        <h2>
          {spec.label}
          <small> · {ids.length}r · t:{renders}</small>
        </h2>
      </header>
      <div ref={flashRef} className="explorer-table-wrap explorer-table-scroll">
        <table className="data-table" data-testid={`exp-${spec.table}-table`}>
          <thead>
            <tr>
              {spec.columns.map((c) => <th key={c.key}>{c.header}</th>)}
              <th className="cell-renders">r</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {ids.map((r) => <Row key={r.id} spec={spec} rowId={r.id} />)}
          </tbody>
        </table>
      </div>
      <NewRowForm spec={spec} fkOptions={fkOptions} />
    </section>
  );
}
