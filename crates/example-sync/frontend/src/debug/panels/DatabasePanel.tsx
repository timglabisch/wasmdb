import { useState } from 'react';
import type { DatabaseDebug, DbInfo, TableInfo } from '../types';
import { getTableRows } from '../wasmDebugApi';

function TableView({ table, dbKind }: { table: TableInfo; dbKind: 'optimistic' | 'confirmed' }) {
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

  return (
    <div className="debug-table-entry">
      <div className="debug-clickable debug-table-header" onClick={handleExpand}>
        <span className="debug-table-name">{table.name}</span>
        <span className="debug-table-count">{table.row_count} rows</span>
        {table.physical_len !== table.row_count && (
          <span className="debug-table-meta">({table.physical_len} physical)</span>
        )}
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

function DbColumn({ info, label, dbKind }: { info: DbInfo; label: string; dbKind: 'optimistic' | 'confirmed' }) {
  return (
    <div className="debug-db-column">
      <div className="debug-db-label">{label}</div>
      {info.tables.map(table => (
        <TableView key={table.name} table={table} dbKind={dbKind} />
      ))}
      {info.tables.length === 0 && <div className="debug-empty">No tables</div>}
    </div>
  );
}

export function DatabasePanel({ data }: { data: DatabaseDebug }) {
  return (
    <div className="debug-panel-db">
      <div className="debug-db-grid">
        <DbColumn info={data.optimistic} label="Optimistic" dbKind="optimistic" />
        <DbColumn info={data.confirmed} label="Confirmed" dbKind="confirmed" />
      </div>
    </div>
  );
}
