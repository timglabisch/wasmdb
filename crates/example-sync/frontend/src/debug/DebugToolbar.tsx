import { useState } from 'react';
import { useDebugSnapshot, useDebugHistory } from './useDebugData';
import { SyncStatusPanel } from './panels/SyncStatusPanel';
import { SubscriptionPanel } from './panels/SubscriptionPanel';
import { EventLogPanel } from './panels/EventLogPanel';
import { DatabasePanel } from './panels/DatabasePanel';
import { PerformancePanel } from './panels/PerformancePanel';
import './DebugToolbar.css';

type PanelId = 'sync' | 'subs' | 'events' | 'db' | 'perf';

export function DebugToolbar() {
  const [open, setOpen] = useState(false);
  const [activePanel, setActivePanel] = useState<PanelId>('sync');
  const snapshot = useDebugSnapshot(open ? 500 : 2000);
  const history = useDebugHistory(snapshot);

  if (!snapshot) return null;

  const tabs: { id: PanelId; label: string; badge?: string }[] = [
    { id: 'sync', label: 'Sync', badge: snapshot.syncStatus.total_pending > 0 ? String(snapshot.syncStatus.total_pending) : undefined },
    { id: 'subs', label: 'Subs', badge: String(snapshot.subscriptions.count) },
    { id: 'events', label: 'Events', badge: String(snapshot.totalEventCount) },
    { id: 'db', label: 'Database' },
    { id: 'perf', label: 'Perf' },
  ];

  return (
    <div className="debug-toolbar" data-open={open}>
      <div className="debug-toolbar-tab" onClick={() => setOpen(!open)}>
        Debug
        {snapshot.syncStatus.total_pending > 0 && (
          <span className="debug-toolbar-tab-badge">{snapshot.syncStatus.total_pending} pending</span>
        )}
      </div>
      {open && (
        <div className="debug-toolbar-content">
          <div className="debug-toolbar-tabs">
            {tabs.map(tab => (
              <button
                key={tab.id}
                className={`debug-toolbar-tab-btn${activePanel === tab.id ? ' active' : ''}`}
                onClick={() => setActivePanel(tab.id)}
              >
                {tab.label}
                {tab.badge && <span className="debug-badge">{tab.badge}</span>}
              </button>
            ))}
          </div>
          <div className="debug-toolbar-panel">
            {activePanel === 'sync' && <SyncStatusPanel data={snapshot.syncStatus} />}
            {activePanel === 'subs' && <SubscriptionPanel data={snapshot.subscriptions} />}
            {activePanel === 'events' && <EventLogPanel events={snapshot.events} />}
            {activePanel === 'db' && <DatabasePanel data={snapshot.database} />}
            {activePanel === 'perf' && <PerformancePanel snapshot={snapshot} history={history} />}
          </div>
        </div>
      )}
    </div>
  );
}
