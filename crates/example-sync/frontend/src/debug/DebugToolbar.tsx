import { useState, useCallback, useRef, useEffect } from 'react';
import { useDebugSnapshot, useDebugHistory } from './useDebugData';
import { SyncStatusPanel } from './panels/SyncStatusPanel';
import { SubscriptionPanel } from './panels/SubscriptionPanel';
import { EventLogPanel } from './panels/EventLogPanel';
import { DatabasePanel } from './panels/DatabasePanel';
import { QueryPanel } from './panels/QueryPanel';
import { PerformancePanel } from './panels/PerformancePanel';
import './DebugToolbar.css';

type PanelId = 'sync' | 'subs' | 'events' | 'db' | 'query' | 'perf';

const MIN_HEIGHT = 100;
const MAX_HEIGHT_RATIO = 0.8;
const DEFAULT_HEIGHT = 280;

export function DebugToolbar() {
  const [open, setOpen] = useState(false);
  const [activePanel, setActivePanel] = useState<PanelId>('sync');
  const [panelHeight, setPanelHeight] = useState(DEFAULT_HEIGHT);
  const dragging = useRef(false);
  const startY = useRef(0);
  const startHeight = useRef(0);
  const toolbarRef = useRef<HTMLDivElement>(null);
  const snapshot = useDebugSnapshot(open ? 500 : 2000);
  const history = useDebugHistory(snapshot);

  useEffect(() => {
    const el = toolbarRef.current;
    if (!el) return;
    const applyPadding = () => {
      document.body.style.paddingBottom = `${el.offsetHeight}px`;
    };
    applyPadding();
    const ro = new ResizeObserver(applyPadding);
    ro.observe(el);
    return () => {
      ro.disconnect();
      document.body.style.paddingBottom = '';
    };
  }, [open, panelHeight]);

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    startY.current = e.clientY;
    startHeight.current = panelHeight;
    document.body.style.cursor = 'ns-resize';
    document.body.style.userSelect = 'none';
  }, [panelHeight]);

  useEffect(() => {
    const onMouseMove = (e: MouseEvent) => {
      if (!dragging.current) return;
      const delta = startY.current - e.clientY;
      const maxHeight = window.innerHeight * MAX_HEIGHT_RATIO;
      const newHeight = Math.min(maxHeight, Math.max(MIN_HEIGHT, startHeight.current + delta));
      setPanelHeight(newHeight);
    };
    const onMouseUp = () => {
      if (!dragging.current) return;
      dragging.current = false;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
    return () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };
  }, []);

  if (!snapshot) return null;

  const tabs: { id: PanelId; label: string; badge?: string }[] = [
    { id: 'sync', label: 'Sync', badge: snapshot.syncStatus.total_pending > 0 ? String(snapshot.syncStatus.total_pending) : undefined },
    { id: 'subs', label: 'Subs', badge: String(snapshot.subscriptions.count) },
    { id: 'events', label: 'Events', badge: String(snapshot.totalEventCount) },
    { id: 'db', label: 'Database' },
    { id: 'query', label: 'Query', badge: snapshot.queryStats.slow_queries > 0 ? String(snapshot.queryStats.slow_queries) : undefined },
    { id: 'perf', label: 'Perf' },
  ];

  return (
    <div className="debug-toolbar" data-open={open} ref={toolbarRef}>
      <div className="debug-toolbar-tab" onClick={() => setOpen(!open)}>
        Debug
        {snapshot.syncStatus.total_pending > 0 && (
          <span className="debug-toolbar-tab-badge">{snapshot.syncStatus.total_pending} pending</span>
        )}
      </div>
      {open && (
        <div className="debug-toolbar-content">
          <div className="debug-resize-handle" onMouseDown={onMouseDown} />
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
          <div className="debug-toolbar-panel" style={{ height: panelHeight }}>
            {activePanel === 'sync' && <SyncStatusPanel data={snapshot.syncStatus} />}
            {activePanel === 'subs' && <SubscriptionPanel data={snapshot.subscriptions} queryStats={snapshot.queryStats} />}
            {activePanel === 'events' && <EventLogPanel events={snapshot.events} />}
            {activePanel === 'db' && <DatabasePanel data={snapshot.database} />}
            {activePanel === 'query' && <QueryPanel queries={snapshot.queryLog} stats={snapshot.queryStats} />}
            {activePanel === 'perf' && <PerformancePanel snapshot={snapshot} history={history} />}
          </div>
        </div>
      )}
    </div>
  );
}
