import { useEffect, useRef, useState } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount, useRenderFlash } from './hooks';
import { QueryErrorBoundary } from './QueryErrorBoundary';
import type { LiveQueryDef } from './types';

let querySeq = 0;
function freshId(): string {
  querySeq += 1;
  return `q-custom-${querySeq}`;
}

function formatValue(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'string') return v;
  return String(v);
}

function ResultView({ sql }: { sql: string }) {
  const rows = useQuery<unknown[]>(sql, (row) => row);
  if (rows.length === 0) return <span className="livestat-empty">—</span>;
  if (rows.length === 1 && rows[0]!.length === 1) {
    return <span className="livestat-scalar">{formatValue(rows[0]![0])}</span>;
  }
  return (
    <span className="livestat-pairs">
      {rows.map((r, i) => (
        <span key={i} className="livestat-pair">
          {r.map((v, j) => (
            <span key={j} className={j === 0 ? 'livestat-pair-key' : 'livestat-pair-val'}>
              {formatValue(v)}
            </span>
          ))}
        </span>
      ))}
    </span>
  );
}

function QueryRow({
  query,
  onChange,
  onDelete,
}: {
  query: LiveQueryDef;
  onChange: (sql: string) => void;
  onDelete: () => void;
}) {
  const flashRef = useRenderFlash<HTMLTableRowElement>();
  const renders = useRenderCount(`Explorer.LiveStats:${query.id}`);
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(query.sql);
  const taRef = useRef<HTMLTextAreaElement | null>(null);

  useEffect(() => { setDraft(query.sql); }, [query.sql]);
  useEffect(() => {
    if (editing && taRef.current) {
      taRef.current.focus();
      taRef.current.select();
    }
  }, [editing]);

  const commit = () => {
    setEditing(false);
    if (draft.trim() !== query.sql) onChange(draft.trim());
  };
  const cancel = () => { setDraft(query.sql); setEditing(false); };

  return (
    <tr ref={flashRef} data-testid={`exp-livestat-${query.id}`}>
      <td className="livestat-sql">
        {editing ? (
          <textarea
            ref={taRef}
            className="livestat-sql-editor"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onBlur={commit}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); commit(); }
              if (e.key === 'Escape') { e.preventDefault(); cancel(); }
            }}
            spellCheck={false}
            rows={Math.min(6, Math.max(1, query.sql.split('\n').length))}
            data-testid={`exp-livestat-edit-${query.id}`}
          />
        ) : (
          <code
            className="livestat-sql-code"
            title="click to edit · ⌘/ctrl+enter to commit · esc to cancel"
            onClick={() => setEditing(true)}
            data-testid={`exp-livestat-sql-${query.id}`}
          >{query.sql}</code>
        )}
      </td>
      <td className="livestat-result">
        <QueryErrorBoundary key={query.sql}>
          <ResultView sql={query.sql} />
        </QueryErrorBoundary>
      </td>
      <td className="livestat-renders">t:{renders}</td>
      <td className="livestat-actions">
        <button
          className="livestat-delete"
          title="delete query"
          onClick={onDelete}
          data-testid={`exp-livestat-delete-${query.id}`}
        >×</button>
      </td>
    </tr>
  );
}

const ADD_FALLBACK = 'SELECT 1';

export function LiveStats({ initial }: { initial: LiveQueryDef[] }) {
  const [queries, setQueries] = useState<LiveQueryDef[]>(initial);

  const update = (id: string, sql: string) => {
    setQueries((prev) => prev.map((q) => (q.id === id ? { ...q, sql } : q)));
  };
  const remove = (id: string) => {
    setQueries((prev) => prev.filter((q) => q.id !== id));
  };
  const add = () => {
    setQueries((prev) => [...prev, { id: freshId(), sql: ADD_FALLBACK }]);
  };

  return (
    <div className="explorer-livestats" data-testid="exp-livestats">
      <div className="explorer-livestats-header">
        <span className="explorer-livestats-title">live queries</span>
        <span className="explorer-livestats-hint">click sql to edit · row flashes on change</span>
        <span className="explorer-livestats-spacer" />
        <button
          className="livestat-add"
          onClick={add}
          data-testid="exp-livestat-add"
        >+ add query</button>
      </div>
      <div className="explorer-livestats-scroll">
        <table className="livestat-table">
          <thead>
            <tr>
              <th>sql</th>
              <th>result</th>
              <th className="livestat-renders">r</th>
              <th className="livestat-actions"></th>
            </tr>
          </thead>
          <tbody>
            {queries.length === 0 && (
              <tr>
                <td colSpan={4} className="livestat-empty-row">
                  no live queries · click <em>+ add query</em>
                </td>
              </tr>
            )}
            {queries.map((q) => (
              <QueryRow
                key={q.id}
                query={q}
                onChange={(sql) => update(q.id, sql)}
                onDelete={() => remove(q.id)}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
