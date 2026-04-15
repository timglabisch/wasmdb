import { useState } from 'react';
import type { QueryTrace, QueryStats, SpanInfo, SpanOperationInfo } from '../types';

const SPAN_COLORS: Record<string, string> = {
  Scan: '#60a5fa',
  Filter: '#4ade80',
  Join: '#fb923c',
  Sort: '#facc15',
  Aggregate: '#c084fc',
  Project: '#94a3b8',
  Materialize: '#f472b6',
  Execute: '#e2e8f0',
};

function formatDuration(us: number): string {
  if (us < 1000) return `${us}us`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`;
  return `${(us / 1_000_000).toFixed(2)}s`;
}

function getOpName(op: SpanOperationInfo): string {
  if (typeof op === 'string') return op;
  if ('Materialize' in op) return 'Materialize';
  if ('Scan' in op) return 'Scan';
  if ('Filter' in op) return 'Filter';
  if ('Join' in op) return 'Join';
  if ('Aggregate' in op) return 'Aggregate';
  if ('Sort' in op) return 'Sort';
  if ('Project' in op) return 'Project';
  return 'Unknown';
}

function formatOp(op: SpanOperationInfo): string {
  if (typeof op === 'string') return op;
  if ('Materialize' in op) return `Materialize step=${op.Materialize.step}`;
  if ('Scan' in op) {
    const s = op.Scan;
    const method = s.method === 'Full'
      ? 'Full'
      : `${s.method.Index.is_hash ? 'Hash' : 'BTree'}(${s.method.Index.columns.join(',')})`;
    return `Scan table=${s.table} method=${method} rows=${s.rows}`;
  }
  if ('Filter' in op) return `Filter rows_in=${op.Filter.rows_in} rows_out=${op.Filter.rows_out}`;
  if ('Join' in op) return `Join rows_out=${op.Join.rows_out}`;
  if ('Aggregate' in op) return `Aggregate groups=${op.Aggregate.groups}`;
  if ('Sort' in op) return `Sort rows=${op.Sort.rows}`;
  if ('Project' in op) return `Project columns=${op.Project.columns} rows=${op.Project.rows}`;
  return 'Unknown';
}

function selectivity(op: SpanOperationInfo): string | null {
  if (typeof op !== 'string' && 'Filter' in op && op.Filter.rows_in > 0) {
    const pct = Math.min(100, (op.Filter.rows_out / op.Filter.rows_in) * 100).toFixed(1);
    return `${pct}%`;
  }
  return null;
}

function SpanNode({ span, depth }: { span: SpanInfo; depth: number }) {
  const name = getOpName(span.operation);
  const color = SPAN_COLORS[name] || '#888';
  const sel = selectivity(span.operation);

  return (
    <>
      <div className="debug-span-node" style={{ paddingLeft: depth * 16 }}>
        <span style={{ color }}>{formatOp(span.operation)}</span>
        <span style={{ color: '#555', marginLeft: 8 }}>({formatDuration(span.duration_us)})</span>
        {sel && <span className="debug-selectivity" style={{ marginLeft: 8 }}>[{sel}]</span>}
      </div>
      {span.children.map((child, i) => (
        <SpanNode key={i} span={child} depth={depth + 1} />
      ))}
    </>
  );
}

function QueryRow({ trace }: { trace: QueryTrace }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div>
      <div
        className={`debug-query-row${trace.is_slow ? ' slow' : ''}`}
        onClick={() => setExpanded(!expanded)}
      >
        <span className="debug-query-duration">{formatDuration(trace.duration_us)}</span>
        <span style={{ color: '#888', minWidth: 32 }}>{trace.row_count}r</span>
        <span style={{ color: '#555', minWidth: 24 }}>{trace.source === 'confirmed' ? 'C' : 'O'}</span>
        <span className="debug-query-sql">{trace.sql}</span>
      </div>
      {expanded && (
        <div className="debug-span-tree">
          {trace.spans.map((span, i) => (
            <SpanNode key={i} span={span} depth={0} />
          ))}
          {trace.spans.length === 0 && (
            <div className="debug-empty">No execution spans (non-SELECT)</div>
          )}
        </div>
      )}
    </div>
  );
}

export function QueryPanel({ queries, stats }: { queries: QueryTrace[]; stats: QueryStats }) {
  const avgDuration = queries.length > 0
    ? queries.reduce((sum, q) => sum + q.duration_us, 0) / queries.length
    : 0;

  return (
    <div className="debug-panel-query">
      <div className="debug-metrics">
        <div className="debug-metric">
          <span className="debug-metric-label">Total Queries</span>
          <span className="debug-metric-value">{stats.total_queries}</span>
        </div>
        <div className="debug-metric">
          <span className="debug-metric-label">Slow Queries</span>
          <span
            className="debug-metric-value"
            data-highlight={stats.slow_queries > 0 ? 'warn' : undefined}
          >
            {stats.slow_queries}
          </span>
        </div>
        <div className="debug-metric">
          <span className="debug-metric-label">Avg Duration</span>
          <span className="debug-metric-value">{formatDuration(Math.round(avgDuration))}</span>
        </div>
      </div>

      <div className="debug-query-list">
        {queries.length === 0 && <div className="debug-empty">No queries recorded</div>}
        {[...queries].reverse().map((trace, i) => (
          <QueryRow key={i} trace={trace} />
        ))}
      </div>
    </div>
  );
}
