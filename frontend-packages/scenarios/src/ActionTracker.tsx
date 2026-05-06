import { createContext, useCallback, useContext, useState, type ReactNode } from 'react';
import type { Scenario } from './types';

export type DiffStatus = 'expected-render' | 'unexpected-render' | 'neutral';

export interface DiffEntry {
  name: string;
  delta: number;
  status: DiffStatus;
}

export interface LastAction {
  label: string;
  ts: number;
  diff: DiffEntry[];
  missingExpected: string[];
}

type TrackFn = (label: string, action: () => void) => void;

const TrackContext = createContext<TrackFn>((_l, a) => a());
const LastContext = createContext<LastAction | null>(null);

export const useAction = () => ({
  track: useContext(TrackContext),
  last: useContext(LastContext),
});

function snapshot(): Record<string, number> {
  const log = (window as unknown as { __renderLog?: Map<string, number> }).__renderLog;
  if (!log) return {};
  return Object.fromEntries(log);
}

/**
 * Pattern matcher for `shouldRender`/`shouldStayQuiet`. Supports:
 *   • exact match            "Counter:00000000-..."
 *   • trailing wildcard      "Counter:*"           (prefix match)
 *   • leading wildcard       "*@msg:M1"            (suffix match)
 *   • surrounding wildcard   "*UserBadge*"         (substring match)
 */
function matchPattern(name: string, pattern: string): boolean {
  const starts = pattern.startsWith('*');
  const ends = pattern.endsWith('*');
  if (starts && ends) return name.includes(pattern.slice(1, -1));
  if (starts) return name.endsWith(pattern.slice(1));
  if (ends) return name.startsWith(pattern.slice(0, -1));
  return name === pattern;
}

function classify(name: string, scenario: Scenario): DiffStatus {
  const sr = scenario.shouldRender ?? [];
  const sq = scenario.shouldStayQuiet ?? [];
  if (sr.some((p) => matchPattern(name, p))) return 'expected-render';
  if (sq.some((p) => matchPattern(name, p))) return 'unexpected-render';
  return 'neutral';
}

interface ProviderProps {
  scenario: Scenario;
  children: ReactNode;
}

export function ActionProvider({ scenario, children }: ProviderProps) {
  const [last, setLast] = useState<LastAction | null>(null);

  const track = useCallback<TrackFn>((label, action) => {
    const before = snapshot();
    try {
      action();
    } catch (e) {
      console.error('action threw', e);
    }
    window.setTimeout(() => {
      const after = snapshot();
      const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
      const diff: DiffEntry[] = [];
      for (const k of keys) {
        const delta = (after[k] ?? 0) - (before[k] ?? 0);
        if (delta === 0) continue;
        diff.push({ name: k, delta, status: classify(k, scenario) });
      }
      const order: Record<DiffStatus, number> = {
        'expected-render': 0,
        'unexpected-render': 1,
        'neutral': 2,
      };
      diff.sort((a, b) => order[a.status] - order[b.status] || a.name.localeCompare(b.name));

      const missingExpected = (scenario.shouldRender ?? []).filter(
        (p) => !diff.some((d) => matchPattern(d.name, p)),
      );

      setLast({ label, ts: Date.now(), diff, missingExpected });
    }, 250);
  }, [scenario]);

  return (
    <TrackContext.Provider value={track}>
      <LastContext.Provider value={last}>
        {children}
      </LastContext.Provider>
    </TrackContext.Provider>
  );
}

const MARK: Record<DiffStatus, string> = {
  'expected-render': '✓',
  'unexpected-render': '✗',
  'neutral': '·',
};

export function LastActionPanel() {
  const { last } = useAction();
  return (
    <aside className="action-panel" data-testid="action-panel">
      <header className="action-panel-header">
        <strong>Live diff</strong>
        <span className="action-panel-hint">click a button → watch what ticks</span>
      </header>
      {!last ? (
        <p className="action-empty">No action yet. Click any button below.</p>
      ) : (
        <>
          <div className="action-label">
            <span className="action-label-bullet">▸</span>
            <span>{last.label}</span>
          </div>
          {last.diff.length === 0 ? (
            <p className="action-empty">All components stayed quiet.</p>
          ) : (
            <ul className="action-diff">
              {last.diff.map((d) => (
                <li key={d.name} className={`diff-row diff-${d.status}`}>
                  <span className="diff-mark" aria-hidden="true">{MARK[d.status]}</span>
                  <span className="diff-name">{d.name}</span>
                  <span className="diff-delta">+{d.delta}</span>
                </li>
              ))}
            </ul>
          )}
          {last.missingExpected.length > 0 && (
            <div className="action-missing">
              <strong>Expected but did not fire:</strong>
              <ul>
                {last.missingExpected.map((p) => (
                  <li key={p}><code>{p}</code></li>
                ))}
              </ul>
            </div>
          )}
        </>
      )}
    </aside>
  );
}
