import { memo, useMemo, useState } from 'react';
import { createStream, flushStream, useQuery } from '@wasmdb/client';
import { useRenderCount, useRenderFlash } from './hooks';
import { EditableNumber } from './EditableNumber';
import { EditableSelect } from './EditableSelect';
import { EditableText } from './EditableText';
import { FkPicker } from './FkPicker';
import type { ColumnSpec, FkResolver, NewFieldSpec, Option, TableSpec } from './types';

type FkResolverMap = Record<string, FkResolver>;

function resolveFk(resolvers: FkResolverMap | undefined, ref: string): FkResolver {
  const r = resolvers?.[ref];
  if (!r) {
    throw new Error(
      `Playground: missing fkResolver for ref="${ref}". ` +
      `Add it to PlaygroundConfig.fkResolvers.`,
    );
  }
  return r;
}

function Cell({
  table,
  rowId,
  col,
  value,
  fkResolvers,
}: {
  table: string;
  rowId: string;
  col: ColumnSpec;
  value: unknown;
  fkResolvers: FkResolverMap | undefined;
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
      return (
        <FkPicker
          resolver={resolveFk(fkResolvers, col.ref)}
          value={String(value ?? '')}
          onSave={(v) => col.onSave!(rowId, v)}
          testid={testid}
        />
      );
    }
  }
}

const Row = memo(function Row({
  spec,
  rowId,
  fkResolvers,
}: {
  spec: TableSpec;
  rowId: string;
  fkResolvers: FkResolverMap | undefined;
}) {
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
          <Cell table={spec.table} rowId={rowId} col={c} value={row[c.key]} fkResolvers={fkResolvers} />
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
  fkResolvers,
}: {
  table: string;
  field: NewFieldSpec;
  value: unknown;
  onChange: (v: unknown) => void;
  onSubmit: () => void;
  fkResolvers: FkResolverMap | undefined;
}) {
  const testid = `exp-${table}-new-${field.key}`;
  if (field.kind === 'fk') {
    return (
      <FkPicker
        resolver={resolveFk(fkResolvers, field.ref)}
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

function NewRowForm({
  spec,
  fkResolvers,
}: {
  spec: TableSpec;
  fkResolvers: FkResolverMap | undefined;
}) {
  const create = spec.create;
  const initial = useMemo<Record<string, unknown>>(() => {
    if (!create) return {};
    const v: Record<string, unknown> = {};
    for (const f of create.fields) v[f.key] = defaultFor(f);
    return v;
  }, [create]);
  const [values, setValues] = useState<Record<string, unknown>>(initial);
  const [bulkCount, setBulkCount] = useState<number>(10);

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

  const submitBulk = async () => {
    if (!valid || bulkCount < 1) return;
    // Batch all bulk inserts onto a single stream so the client sends ONE
    // HTTP request (instead of N). Stream's batchCount = bulkCount caps the
    // outgoing batch at exactly the number of commands we enqueue.
    const streamId = createStream(bulkCount, 0, 0);
    for (let i = 0; i < bulkCount; i++) {
      const v: Record<string, unknown> = { ...values };
      for (const f of create.fields) {
        if (f.kind === 'text' && typeof v[f.key] === 'string' && (v[f.key] as string).trim()) {
          v[f.key] = `${(v[f.key] as string).trim()} ${i + 1}`;
        }
      }
      create.fire(v, streamId);
    }
    await flushStream(streamId);
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
          fkResolvers={fkResolvers}
        />
      ))}
      <button
        onClick={submit}
        disabled={!valid}
        data-testid={`exp-${spec.table}-create-submit`}
      >{create.label}</button>
      <span className="explorer-new-bulk">
        <span className="explorer-new-bulk-times">×</span>
        <input
          type="number"
          min={1}
          max={10000}
          value={bulkCount}
          onChange={(e) => setBulkCount(Math.max(1, Number(e.target.value) || 1))}
          className="explorer-new-bulk-count"
          data-testid={`exp-${spec.table}-bulk-count`}
        />
        <button
          onClick={submitBulk}
          disabled={!valid}
          className="explorer-new-bulk-submit"
          title={`spawn ${bulkCount} rows · text fields get suffixed " 1", " 2" …`}
          data-testid={`exp-${spec.table}-bulk-submit`}
        >bulk</button>
      </span>
    </div>
  );
}

const PAGE_SIZES = [25, 50, 100, 500] as const;

function Pagination({
  table,
  total,
  page,
  pageSize,
  onPage,
  onPageSize,
}: {
  table: string;
  total: number;
  page: number;
  pageSize: number | 'all';
  onPage: (p: number) => void;
  onPageSize: (s: number | 'all') => void;
}) {
  const effectiveSize = pageSize === 'all' ? Math.max(total, 1) : pageSize;
  const totalPages = Math.max(1, Math.ceil(total / effectiveSize));
  const safePage = Math.min(page, totalPages - 1);
  const start = total === 0 ? 0 : safePage * effectiveSize + 1;
  const end = Math.min(total, (safePage + 1) * effectiveSize);
  return (
    <div className="explorer-pagination" data-testid={`exp-${table}-pagination`}>
      <button
        onClick={() => onPage(0)}
        disabled={safePage === 0}
        title="first"
        data-testid={`exp-${table}-page-first`}
      >«</button>
      <button
        onClick={() => onPage(Math.max(0, safePage - 1))}
        disabled={safePage === 0}
        title="prev"
        data-testid={`exp-${table}-page-prev`}
      >‹</button>
      <span className="explorer-pagination-info">
        {start}–{end} of {total} · page {safePage + 1}/{totalPages}
      </span>
      <button
        onClick={() => onPage(Math.min(totalPages - 1, safePage + 1))}
        disabled={safePage >= totalPages - 1}
        title="next"
        data-testid={`exp-${table}-page-next`}
      >›</button>
      <button
        onClick={() => onPage(totalPages - 1)}
        disabled={safePage >= totalPages - 1}
        title="last"
        data-testid={`exp-${table}-page-last`}
      >»</button>
      <span className="explorer-pagination-spacer" />
      <select
        value={pageSize}
        onChange={(e) => onPageSize(e.target.value === 'all' ? 'all' : Number(e.target.value))}
        className="explorer-pagination-size"
        data-testid={`exp-${table}-page-size`}
      >
        {PAGE_SIZES.map((n) => <option key={n} value={n}>{n}/page</option>)}
        <option value="all">all</option>
      </select>
    </div>
  );
}

export function DataTable({
  spec,
  fkResolvers,
}: {
  spec: TableSpec;
  fkResolvers: FkResolverMap | undefined;
}) {
  const renders = useRenderCount(`Explorer.${spec.table}.Table`);
  const flashRef = useRenderFlash<HTMLDivElement>();
  const orderBy = spec.orderBy ?? `${spec.table}.id`;
  const ids = useQuery<{ id: string }>(
    `SELECT REACTIVE(${spec.table}.id), ${spec.table}.id FROM ${spec.table} ORDER BY ${orderBy}`,
    ([_r, id]) => ({ id: id as string }),
  );
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState<number | 'all'>(25);
  const visibleIds = useMemo(() => {
    if (pageSize === 'all') return ids;
    const start = page * pageSize;
    return ids.slice(start, start + pageSize);
  }, [ids, page, pageSize]);

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
            {visibleIds.map((r) => <Row key={r.id} spec={spec} rowId={r.id} fkResolvers={fkResolvers} />)}
          </tbody>
        </table>
      </div>
      <Pagination
        table={spec.table}
        total={ids.length}
        page={page}
        pageSize={pageSize}
        onPage={setPage}
        onPageSize={(s) => { setPageSize(s); setPage(0); }}
      />
      <NewRowForm spec={spec} fkResolvers={fkResolvers} />
    </section>
  );
}

export type { Option };
