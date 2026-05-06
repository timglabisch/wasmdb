import { useState } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount, useRenderFlash } from './hooks';
import { QueryErrorBoundary } from './QueryErrorBoundary';
import type { QueryPreset } from './types';

function ResultTable({ sql }: { sql: string }) {
  const renders = useRenderCount(`Explorer.CustomQuery:${sql}`);
  const flashRef = useRenderFlash<HTMLDivElement>();
  const rows = useQuery<unknown[]>(sql, (row) => row);
  const cols = rows[0] ? rows[0].length : 0;
  return (
    <div ref={flashRef} className="explorer-table-wrap explorer-table-scroll">
      <div className="custom-query-stats" data-testid="exp-custom-query-stats">
        {rows.length} rows · {cols} cols · t:{renders}
      </div>
      {rows.length === 0 ? (
        <div className="custom-query-empty">no rows</div>
      ) : (
        <table className="data-table" data-testid="exp-custom-query-table">
          <thead>
            <tr>{Array.from({ length: cols }).map((_, i) => <th key={i}>col{i}</th>)}</tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i}>
                {r.map((v, j) => (
                  <td key={j} className="cell-mono"><code>{stringify(v)}</code></td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function stringify(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'string') return v;
  return String(v);
}

const FALLBACK_SQL = '-- write any SELECT, run with REACTIVE(...) to make it live\n';

export function CustomQuery({ presets }: { presets: QueryPreset[] }) {
  const initial = presets[0]?.sql ?? FALLBACK_SQL;
  const [draft, setDraft] = useState(initial);
  const [committed, setCommitted] = useState<string | null>(presets[0]?.sql ?? null);
  return (
    <section className="explorer-table">
      <header className="explorer-table-header">
        <h2>custom reactive query <small>· write any SELECT, get a live view</small></h2>
      </header>
      <div className="custom-query-form">
        <textarea
          className="custom-query-input"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          spellCheck={false}
          rows={4}
          data-testid="exp-custom-query-input"
        />
        <div className="custom-query-controls">
          <button
            className="custom-query-run"
            data-testid="exp-custom-query-run"
            onClick={() => setCommitted(draft)}
          >Run</button>
          {presets.length > 0 && (
            <span className="custom-query-presets">
              {presets.map((p) => (
                <button
                  key={p.label}
                  className="custom-query-preset"
                  onClick={() => { setDraft(p.sql); setCommitted(p.sql); }}
                >{p.label}</button>
              ))}
            </span>
          )}
        </div>
      </div>
      {committed && (
        <QueryErrorBoundary key={committed}>
          <ResultTable sql={committed} />
        </QueryErrorBoundary>
      )}
    </section>
  );
}
