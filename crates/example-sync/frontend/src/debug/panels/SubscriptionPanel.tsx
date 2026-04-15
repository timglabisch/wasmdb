import type { SubscriptionDebug } from '../types';

export function SubscriptionPanel({ data }: { data: SubscriptionDebug }) {
  const sorted = [...data.subscriptions].sort((a, b) => {
    const ca = data.notification_counts[a.id] ?? 0;
    const cb = data.notification_counts[b.id] ?? 0;
    return cb - ca;
  });

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
              <th>Tables</th>
              <th>Notifications</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(sub => (
              <tr key={sub.id}>
                <td>#{sub.id}</td>
                <td>{sub.tables.join(', ') || '-'}</td>
                <td>{data.notification_counts[sub.id] ?? 0}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <div className="debug-empty">No subscriptions</div>
      )}
    </div>
  );
}
