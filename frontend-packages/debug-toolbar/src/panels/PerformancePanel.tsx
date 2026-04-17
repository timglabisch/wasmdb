import type { DebugSnapshot } from '../types';
import type { HistoryPoint } from '../useDebugData';

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function Sparkline({ data, accessor, color }: { data: HistoryPoint[]; accessor: (p: HistoryPoint) => number; color: string }) {
  if (data.length < 2) return <span className="debug-spark-empty">-</span>;

  const values = data.map(accessor);
  const max = Math.max(...values, 1);
  const barWidth = Math.max(1, Math.floor(200 / data.length));

  return (
    <div className="debug-sparkline" style={{ width: 200, height: 24 }}>
      {values.map((v, i) => (
        <div
          key={i}
          className="debug-spark-bar"
          style={{
            width: barWidth,
            height: `${(v / max) * 100}%`,
            backgroundColor: color,
          }}
        />
      ))}
    </div>
  );
}

function InvalidationChart({ counts }: { counts: Record<string, number> }) {
  const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]);
  if (entries.length === 0) return <span className="debug-spark-empty">No invalidations</span>;

  const max = entries[0][1];

  return (
    <div className="debug-bar-chart">
      {entries.map(([table, count]) => (
        <div key={table} className="debug-bar-row">
          <span className="debug-bar-label">{table}</span>
          <div className="debug-bar-fill" style={{ width: `${(count / max) * 100}px` }} />
          <span style={{ color: '#888', fontSize: 10, marginLeft: 4 }}>{count}</span>
        </div>
      ))}
    </div>
  );
}

export function PerformancePanel({ snapshot, history }: { snapshot: DebugSnapshot; history: HistoryPoint[] }) {
  const avgQueryDuration = snapshot.queryLog.length > 0
    ? snapshot.queryLog.reduce((sum, q) => sum + q.duration_us, 0) / snapshot.queryLog.length
    : 0;

  return (
    <div className="debug-panel-perf">
      <div className="debug-metrics">
        <div className="debug-metric">
          <span className="debug-metric-label">WASM Memory</span>
          <span className="debug-metric-value">{formatBytes(snapshot.wasmMemoryBytes)}</span>
        </div>
        <div className="debug-metric">
          <span className="debug-metric-label">Subscriptions</span>
          <span className="debug-metric-value">{snapshot.subscriptions.count}</span>
        </div>
        <div className="debug-metric">
          <span className="debug-metric-label">Total Events</span>
          <span className="debug-metric-value">{snapshot.totalEventCount}</span>
        </div>
        <div className="debug-metric">
          <span className="debug-metric-label">Queries</span>
          <span className="debug-metric-value">{snapshot.queryStats.total_queries}</span>
        </div>
        <div className="debug-metric">
          <span className="debug-metric-label">Slow Queries</span>
          <span
            className="debug-metric-value"
            data-highlight={snapshot.queryStats.slow_queries > 0 ? 'warn' : undefined}
          >
            {snapshot.queryStats.slow_queries}
          </span>
        </div>
        <div className="debug-metric">
          <span className="debug-metric-label">Avg Query</span>
          <span className="debug-metric-value">
            {avgQueryDuration < 1000 ? `${Math.round(avgQueryDuration)}us` : `${(avgQueryDuration / 1000).toFixed(1)}ms`}
          </span>
        </div>
      </div>

      <div className="debug-charts">
        <div className="debug-chart">
          <span className="debug-chart-label">Memory</span>
          <Sparkline data={history} accessor={p => p.memory} color="#60a5fa" />
        </div>
        <div className="debug-chart">
          <span className="debug-chart-label">Pending</span>
          <Sparkline data={history} accessor={p => p.pendingCount} color="#facc15" />
        </div>
        <div className="debug-chart">
          <span className="debug-chart-label">Subscriptions</span>
          <Sparkline data={history} accessor={p => p.subCount} color="#c084fc" />
        </div>
        <div className="debug-chart">
          <span className="debug-chart-label">Queries</span>
          <Sparkline data={history} accessor={p => p.queryCount} color="#4ade80" />
        </div>
      </div>

      <div style={{ marginTop: 12 }}>
        <span className="debug-chart-label">Table Invalidations</span>
        <div style={{ marginTop: 4 }}>
          <InvalidationChart counts={snapshot.queryStats.table_invalidation_counts} />
        </div>
      </div>
    </div>
  );
}
