import { useState } from 'react';
import { CustomQuery } from './CustomQuery';
import { DataTable } from './DataTable';
import { LiveStats } from './LiveStats';
import { ALL_SPECS } from './tableSpecs';
import type { TableSpec } from './types';

type Tab =
  | { id: string; kind: 'table'; spec: TableSpec }
  | { id: string; kind: 'query'; title: string };

let queryTabSeq = 0;

function makeQueryTab(): Tab {
  queryTabSeq += 1;
  return { id: `query-${queryTabSeq}`, kind: 'query', title: `console ${queryTabSeq}` };
}

export function Explorer() {
  const [tabs, setTabs] = useState<Tab[]>(() => [
    { id: `table-${ALL_SPECS[0]!.table}`, kind: 'table', spec: ALL_SPECS[0]! },
  ]);
  const [activeId, setActiveId] = useState<string>(() => `table-${ALL_SPECS[0]!.table}`);

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
    <div className="explorer-shell">
      <aside className="explorer-sidebar">
        <div className="explorer-sidebar-header">
          <span className="explorer-db-icon">▾</span>
          <span className="explorer-db-name">wasmdb</span>
        </div>
        <ul className="explorer-tree">
          <li className="explorer-tree-group">
            <span className="explorer-tree-group-label">▾ tables</span>
            <ul className="explorer-tree-children">
              {ALL_SPECS.map((spec) => {
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
          <a href="#/" data-testid="explorer-back">← scenarios</a>
        </div>
      </aside>

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
          {active?.kind === 'table' && <DataTable key={active.id} spec={active.spec} />}
          {active?.kind === 'query' && <CustomQuery key={active.id} />}
        </div>

        <LiveStats />
      </main>
    </div>
  );
}
