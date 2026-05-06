import { useEffect, useMemo, useState } from 'react';
import { DebugToolbar } from '@wasmdb/debug-toolbar';
import { CustomQuery } from './CustomQuery';
import { DataTable } from './DataTable';
import { LiveStats } from './LiveStats';
import { Splitter } from './Splitter';
import type { PlaygroundConfig, TableSpec } from './types';
import './Playground.css';

const SIDEBAR_MIN = 140;
const SIDEBAR_MAX = 480;
const LIVESTATS_MIN = 32;
const LIVESTATS_MAX_RESERVE = 200;
const STORAGE_KEY = 'wasmdb-playground.layout.v1';

interface Layout { sidebarW: number; liveStatsH: number }

function loadLayout(): Layout {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const v = JSON.parse(raw) as Partial<Layout>;
      return {
        sidebarW: typeof v.sidebarW === 'number' ? v.sidebarW : 220,
        liveStatsH: typeof v.liveStatsH === 'number' ? v.liveStatsH : 180,
      };
    }
  } catch { /* ignore */ }
  return { sidebarW: 220, liveStatsH: 180 };
}

function saveLayout(layout: Layout) {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(layout)); } catch { /* ignore */ }
}

type Tab =
  | { id: string; kind: 'table'; spec: TableSpec }
  | { id: string; kind: 'query'; title: string };

let queryTabSeq = 0;

function makeQueryTab(): Tab {
  queryTabSeq += 1;
  return { id: `query-${queryTabSeq}`, kind: 'query', title: `console ${queryTabSeq}` };
}

export function Playground({ config }: { config: PlaygroundConfig }) {
  const {
    specs,
    fkResolvers,
    customQueryPresets = [],
    liveQueries = [],
    backHref = '#/',
    backLabel = '← back',
  } = config;

  const initialTab = useMemo<Tab[]>(() => {
    if (specs.length === 0) return [];
    const first = specs[0]!;
    return [{ id: `table-${first.table}`, kind: 'table', spec: first }];
  }, [specs]);

  const [tabs, setTabs] = useState<Tab[]>(initialTab);
  const [activeId, setActiveId] = useState<string>(() => initialTab[0]?.id ?? '');
  const [layout, setLayout] = useState<Layout>(loadLayout);
  const [toolbarH, setToolbarH] = useState(0);

  useEffect(() => {
    let ro: ResizeObserver | null = null;
    const attach = (el: HTMLElement) => {
      setToolbarH(Math.round(el.getBoundingClientRect().height));
      ro = new ResizeObserver((entries) => {
        for (const e of entries) setToolbarH(Math.round(e.contentRect.height));
      });
      ro.observe(el);
    };
    const el = document.querySelector<HTMLElement>('.debug-toolbar');
    if (el) {
      attach(el);
    } else {
      const mo = new MutationObserver(() => {
        const found = document.querySelector<HTMLElement>('.debug-toolbar');
        if (found) { attach(found); mo.disconnect(); }
      });
      mo.observe(document.body, { childList: true, subtree: true });
      return () => { mo.disconnect(); ro?.disconnect(); };
    }
    return () => { ro?.disconnect(); };
  }, []);

  const dragSidebar = (delta: number) => {
    setLayout((l) => {
      const next = { ...l, sidebarW: Math.max(SIDEBAR_MIN, Math.min(SIDEBAR_MAX, l.sidebarW + delta)) };
      saveLayout(next);
      return next;
    });
  };
  const dragLiveStats = (delta: number) => {
    setLayout((l) => {
      const max = Math.max(LIVESTATS_MIN, window.innerHeight - LIVESTATS_MAX_RESERVE);
      const next = { ...l, liveStatsH: Math.max(LIVESTATS_MIN, Math.min(max, l.liveStatsH - delta)) };
      saveLayout(next);
      return next;
    });
  };

  const openTable = (spec: TableSpec) => {
    const id = `table-${spec.table}`;
    setTabs((prev) => (prev.some((t) => t.id === id) ? prev : [...prev, { id, kind: 'table', spec }]));
    setActiveId(id);
  };

  const openQuery = () => {
    const tab = makeQueryTab();
    setTabs((prev) => [...prev, tab]);
    setActiveId(tab.id);
  };

  const closeTab = (id: string) => {
    setTabs((prev) => {
      const idx = prev.findIndex((t) => t.id === id);
      if (idx === -1) return prev;
      const next = prev.filter((t) => t.id !== id);
      if (id === activeId) {
        const fallback = next[idx] ?? next[idx - 1] ?? next[0];
        setActiveId(fallback?.id ?? '');
      }
      return next;
    });
  };

  const active = tabs.find((t) => t.id === activeId) ?? null;

  return (
    <div
      className="explorer-shell"
      style={{
        gridTemplateColumns: `${layout.sidebarW}px 4px minmax(0, 1fr)`,
        bottom: `${toolbarH}px`,
        height: 'auto',
        maxHeight: 'none',
      }}
    >
      <aside className="explorer-sidebar">
        <div className="explorer-sidebar-header">
          <span className="explorer-db-icon">▾</span>
          <span className="explorer-db-name">wasmdb</span>
        </div>
        <ul className="explorer-tree">
          <li className="explorer-tree-group">
            <span className="explorer-tree-group-label">▾ tables</span>
            <ul className="explorer-tree-children">
              {specs.map((spec) => {
                const id = `table-${spec.table}`;
                const isOpen = tabs.some((t) => t.id === id);
                const isActive = activeId === id;
                return (
                  <li
                    key={spec.table}
                    className={
                      'explorer-tree-item' +
                      (isActive ? ' is-active' : '') +
                      (isOpen ? ' is-open' : '')
                    }
                    onDoubleClick={() => openTable(spec)}
                    onClick={() => openTable(spec)}
                    data-testid={`exp-tree-${spec.table}`}
                  >
                    <span className="explorer-tree-icon">▦</span>
                    <span className="explorer-tree-label">{spec.label}</span>
                  </li>
                );
              })}
            </ul>
          </li>
          <li className="explorer-tree-group">
            <span className="explorer-tree-group-label">▾ consoles</span>
            <ul className="explorer-tree-children">
              <li
                className="explorer-tree-item explorer-tree-item-action"
                onClick={openQuery}
                data-testid="exp-tree-new-query"
              >
                <span className="explorer-tree-icon">+</span>
                <span className="explorer-tree-label">new query console</span>
              </li>
            </ul>
          </li>
        </ul>
        <div className="explorer-sidebar-footer">
          <a href={backHref} data-testid="explorer-back">{backLabel}</a>
        </div>
      </aside>

      <Splitter direction="horizontal" onDrag={dragSidebar} testid="exp-splitter-sidebar" />

      <main className="explorer-main">
        <div className="explorer-tabs" role="tablist">
          {tabs.map((t) => {
            const isActive = t.id === activeId;
            const label = t.kind === 'table' ? t.spec.label : t.title;
            const icon = t.kind === 'table' ? '▦' : '⌘';
            return (
              <div
                key={t.id}
                role="tab"
                className={'explorer-tab' + (isActive ? ' is-active' : '')}
                onClick={() => setActiveId(t.id)}
                data-testid={`exp-tab-${t.id}`}
              >
                <span className="explorer-tab-icon">{icon}</span>
                <span className="explorer-tab-label">{label}</span>
                <button
                  className="explorer-tab-close"
                  onClick={(e) => { e.stopPropagation(); closeTab(t.id); }}
                  title="close tab"
                  data-testid={`exp-tab-close-${t.id}`}
                >×</button>
              </div>
            );
          })}
          <button
            className="explorer-tab-new"
            onClick={openQuery}
            title="new query console"
            data-testid="exp-tab-new"
          >+</button>
        </div>

        <div className="explorer-tab-body">
          {active === null && <div className="explorer-empty">no tab open · pick a table from the tree</div>}
          {active?.kind === 'table' && <DataTable key={active.id} spec={active.spec} fkResolvers={fkResolvers} />}
          {active?.kind === 'query' && <CustomQuery key={active.id} presets={customQueryPresets} />}
        </div>

        <Splitter direction="vertical" onDrag={dragLiveStats} testid="exp-splitter-livestats" />

        <div className="explorer-livestats-wrap" style={{ height: layout.liveStatsH }}>
          <LiveStats initial={liveQueries} />
        </div>
      </main>

      <DebugToolbar />
    </div>
  );
}
