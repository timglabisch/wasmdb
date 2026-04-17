import { useRef, useEffect } from 'react';
import type { DebugEvent } from '../types';
import { clearLog } from '../wasmDebugApi';

const EVENT_COLORS: Record<string, string> = {
  Execute: '#60a5fa',
  FetchStart: '#fbbf24',
  FetchEnd: '#fbbf24',
  Confirmed: '#4ade80',
  Rejected: '#f87171',
  Notification: '#c084fc',
  SubscriptionCreated: '#94a3b8',
  SubscriptionRemoved: '#94a3b8',
  QueryExecuted: '#38bdf8',
  SlowQuery: '#f87171',
};

function formatTime(ms: number, refMs: number): string {
  const delta = ms - refMs;
  if (Math.abs(delta) < 1000) return `+${delta.toFixed(0)}ms`;
  return `+${(delta / 1000).toFixed(1)}s`;
}

function eventSummary(event: DebugEvent): string {
  switch (event.kind) {
    case 'Execute':
      return `stream#${event.stream_id} ${event.command_json.slice(0, 60)} (${event.zset_entry_count} entries)`;
    case 'FetchStart':
      return `stream#${event.stream_id} ${event.request_bytes}B`;
    case 'FetchEnd':
      return `stream#${event.stream_id} ${event.response_bytes}B ${event.latency_ms.toFixed(0)}ms`;
    case 'Confirmed':
      return `stream#${event.stream_id}`;
    case 'Rejected':
      return `stream#${event.stream_id} ${event.reason}`;
    case 'Notification':
      return `#${event.sub_id} triggered=${event.triggered_count}`;
    case 'SubscriptionCreated':
      return `#${event.sub_id} ${event.sql.slice(0, 50)} [${event.tables.join(',')}]`;
    case 'SubscriptionRemoved':
      return `#${event.sub_id}`;
    case 'QueryExecuted': {
      const dur = event.duration_us < 1000 ? `${event.duration_us}us` : `${(event.duration_us / 1000).toFixed(1)}ms`;
      return `[${event.source}] ${event.row_count}r ${dur} ${event.sql.slice(0, 50)}`;
    }
    case 'SlowQuery': {
      const dur = event.duration_us < 1000 ? `${event.duration_us}us` : `${(event.duration_us / 1000).toFixed(1)}ms`;
      return `SLOW ${dur} ${event.sql.slice(0, 60)}`;
    }
  }
}

export function EventLogPanel({ events }: { events: DebugEvent[] }) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const refMs = events.length > 0 ? events[0].timestamp_ms : 0;

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [events.length]);

  return (
    <div className="debug-panel-events">
      <div className="debug-panel-header">
        <span>{events.length} events</span>
        <button className="debug-btn-small" onClick={clearLog}>Clear</button>
      </div>
      <div className="debug-event-list" ref={scrollRef}>
        {events.length === 0 ? (
          <div className="debug-empty">No events yet</div>
        ) : (
          events.map((event, i) => (
            <div key={i} className="debug-event-row">
              <span className="debug-event-time">{formatTime(event.timestamp_ms, refMs)}</span>
              <span className="debug-event-kind" style={{ color: EVENT_COLORS[event.kind] }}>
                {event.kind}
              </span>
              <span className="debug-event-summary">{eventSummary(event)}</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
