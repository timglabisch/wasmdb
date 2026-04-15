import type { SubscriptionDebug, QueryStats } from '../types';

export function SubscriptionPanel({ data, queryStats }: { data: SubscriptionDebug; queryStats: QueryStats }) {
  const sorted = [...data.subscriptions].sort((a, b) => a.id - b.id);

  const tableInvalidations = Object.entries(queryStats.table_invalidation_counts)
    .sort((a, b) => b[1] - a[1]);

  return (
    <div className="debug-panel-subs">
      <div className="debug-metrics">
        <div className="debug-metric">
          <span className="debug-metric-label">Active</span>
          <span className="debug-metric-value">{data.count}</span>
        </div>
        <div className="debug-metric">
          <span className="debug-metric-label">Reverse Index</span>
          <span className="debug-metric-value">{data.reverse_index_size}</span>
        </div>
      </div>

      {sorted.length > 0 ? (
        <table className="debug-table">
          <thead>
            <tr>
              <th>SubId</th>
              <th>SQL</th>
              <th>Tables</th>
              <th>Notifications</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(sub => (
              <tr key={sub.id}>
                <td>#{sub.id}</td>
                <td className="debug-query-sql">{sub.sql || '-'}</td>
                <td>{sub.tables.join(', ') || '-'}</td>
                <td>{data.notification_counts[sub.id] ?? 0}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div className="debug-empty">No subscriptions</div>
      )}

      {tableInvalidations.length > 0 && (
        <>
          <div style={{ marginTop: 12, color: '#555', fontSize: 10, textTransform: 'uppercase', marginBottom: 4 }}>
            Per-Table Invalidations
          </div>
          <table className="debug-table">
            <thead>
              <tr>
                <th>Table</th>
                <th>Invalidations</th>
              </tr>
            </thead>
            <tbody>
              {tableInvalidations.map(([table, count]) => (
                <tr key={table}>
                  <td>{table}</td>
                  <td>{count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
