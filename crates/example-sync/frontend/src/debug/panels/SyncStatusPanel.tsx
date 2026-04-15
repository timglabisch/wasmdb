import { useState } from 'react';
import type { SyncStatus } from '../types';

export function SyncStatusPanel({ data }: { data: SyncStatus }) {
  const [expandedStream, setExpandedStream] = useState<number | null>(null);

  return (
    <div className="debug-panel-sync">
      <div className="debug-metrics">
        <div className="debug-metric">
          <span className="debug-metric-label">Streams</span>
          <span className="debug-metric-value">{data.stream_count}</span>
        </div>
        <div className="debug-metric">
          <span className="debug-metric-label">Total Pending</span>
          <span className="debug-metric-value" data-highlight={data.total_pending > 0 ? 'warn' : undefined}>
            {data.total_pending}
          </span>
        </div>
      </div>

      {data.streams.length > 0 && (
        <table className="debug-table">
          <thead>
            <tr>
              <th>Stream</th>
              <th>Pending</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {[...data.streams].sort((a, b) => a.id - b.id).map(stream => (
              <>
                <tr
                  key={stream.id}
                  className="debug-clickable"
                  onClick={() => setExpandedStream(expandedStream === stream.id ? null : stream.id)}
                >
                  <td>#{stream.id}</td>
                  <td>{stream.pending_count}</td>
                  <td>{stream.is_idle ? 'idle' : 'active'}</td>
                </tr>
                {expandedStream === stream.id && stream.pending.length > 0 && (
                  <tr key={`${stream.id}-detail`}>
                    <td colSpan={3} className="debug-detail-cell">
                      {stream.pending.map((p, i) => (
                        <div key={i} className="debug-detail-row">
                          seq={p.seq_no} zset_entries={p.zset_entries}
                        </div>
                      ))}
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      )}

      {data.streams.length === 0 && (
        <div className="debug-empty">No active streams</div>
      )}
    </div>
  );
}
