import { useState } from 'react';
import type { DatabaseDebug, DbInfo, TableInfo } from '../types';
import { getTableRows } from '../wasmDebugApi';

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function FragBar({ table }: { table: TableInfo }) {
  if (table.physical_len === 0) return null;
  const livePct = ((table.row_count / table.physical_len) * 100);
  const deadPct = 100 - livePct;
  if (deadPct < 0.5) return null;
  return (
    <span className="debug-frag-bar" title={`${table.deleted_count} deleted (${deadPct.toFixed(0)}%)`}>
      <span className="debug-frag-live" style={{ width: `${livePct}%` }} />
      <span className="debug-frag-dead" style={{ width: `${deadPct}%` }} />
    </span>
  );
}

function TableView({ table, dbKind, otherRowCount }: { table: TableInfo; dbKind: 'optimistic' | 'confirmed'; otherRowCount?: number }) {
  const [expanded, setExpanded] = useState(false);
  const [rows, setRows] = useState<any[][] | null>(null);

  const handleExpand = () => {
    if (!expanded) {
      try {
        setRows(getTableRows(table.name, dbKind, 100));
      } catch {
        setRows([]);
      }
    }
    setExpanded(!expanded);
  };

  const diffStyle = otherRowCount !== undefined && otherRowCount !== table.row_count
    ? { color: table.row_count > otherRowCount ? '#4ade80' : '#f87171' }
    : undefined;

  return (
    <div className="debug-table-entry">
      <div className="debug-clickable debug-table-header" onClick={handleExpand}>
        <span className="debug-table-name">{table.name}</span>
        <span className="debug-table-count" style={diffStyle}>{table.row_count} rows</span>
        <FragBar table={table} />
        {table.physical_len !== table.row_count && (
          <span className="debug-table-meta">({table.physical_len} physical)</span>
        )}
        <span className="debug-memory-tag">{formatBytes(table.estimated_memory_bytes)}</span>
      </div>
      {expanded && (
        <div className="debug-table-detail">
          <div className="debug-schema">
            {table.columns.map((col, i) => (
              <span key={i} className="debug-col-tag">
                {col.name}: {col.data_type}{col.nullable ? '?' : ''}
              </span>
            ))}
            <span className="debug-col-tag debug-col-meta">{table.index_count} indexes</span>
          </div>

          {table.indexes.length > 0 && (
            <table className="debug-table" style={{ marginBottom: 8 }}>
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Columns</th>
                  <th>Keys</th>
                </tr>
              </thead>
              <tbody>
                {table.indexes.map((idx, i) => (
                  <tr key={i}>
                    <td style={{ color: idx.index_type === 'Hash' ? '#fb923c' : '#60a5fa' }}>{idx.index_type}</td>
                    <td>{idx.columns.join(', ')}</td>
                    <td>{idx.key_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}

          {rows && rows.length > 0 && (
            <table className="debug-table debug-rows-table">
              <thead>
                <tr>
                  {table.columns.map((col, i) => (
                    <th key={i}>{col.name}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((row, i) => (
                  <tr key={i}>
                    {row.map((cell, j) => (
                      <td key={j}>{cell === null ? 'NULL' : String(cell)}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {rows && rows.length === 0 && (
            <div className="debug-empty">Empty table</div>
          )}
        </div>
      )}
    </div>
  );
}

function DbColumn({ info, label, dbKind, otherInfo }: { info: DbInfo; label: string; dbKind: 'optimistic' | 'confirmed'; otherInfo?: DbInfo }) {
  return (
    <div className="debug-db-column">
      <div className="debug-db-label">{label}</div>
      {info.tables.map(table => {
        const other = otherInfo?.tables.find(t => t.name === table.name);
        return (
          <TableView
            key={table.name}
            table={table}
            dbKind={dbKind}
            otherRowCount={other?.row_count}
          />
        );
      })}
      {info.tables.length === 0 && <div className="debug-empty">No tables</div>}
    </div>
  );
}

export function DatabasePanel({ data }: { data: DatabaseDebug }) {
  return (
    <div className="debug-panel-db">
      <div className="debug-db-grid">
        <DbColumn info={data.optimistic} label="Optimistic" dbKind="optimistic" otherInfo={data.confirmed} />
        <DbColumn info={data.confirmed} label="Confirmed" dbKind="confirmed" otherInfo={data.optimistic} />
      </div>
    </div>
  );
}
