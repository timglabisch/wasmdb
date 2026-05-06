import type { Scenario, ScenarioCategory } from './types';
import { CATEGORY_LABEL, CATEGORY_ORDER } from './types';
import './Scenarios.css';

export interface PlaygroundLink {
  href: string;
  label?: string;
  description?: string;
}

interface Props {
  scenarios: Scenario[];
  /** Optional "Open data playground" card rendered below the header. */
  playground?: PlaygroundLink;
  title?: string;
  intro?: React.ReactNode;
}

/**
 * Landing page. Lists every scenario grouped by concept-category. Each
 * entry links to its self-contained page (`#/<scenario.id>`).
 */
export function ScenarioIndex({
  scenarios,
  playground,
  title = 'Scenarios',
  intro,
}: Props) {
  const grouped = groupByCategory(scenarios);
  return (
    <div className="scenario-index">
      <header>
        <h1>{title}</h1>
        {intro ?? (
          <p>
            Each link opens a self-contained page mirroring one Playwright
            spec. Components flash <span className="hint-flash">yellow</span>
            when they re-render, and a <strong>live diff panel</strong> shows
            what ticked after each click — marked ✓ if expected, ✗ if it
            broke isolation. Try predicting first, click, watch, then reveal
            the answer.
          </p>
        )}
        {playground && (
          <p className="scenario-index-playground">
            Or jump into the{' '}
            <a href={playground.href} data-testid="playground-link">
              <strong>{playground.label ?? 'data playground'}</strong>
            </a>
            {' — '}
            {playground.description ??
              'live tables for every domain table, inline editing, same in-memory database the scenarios use.'}
          </p>
        )}
      </header>
      {CATEGORY_ORDER.map((cat) => {
        const list = grouped[cat];
        if (list.length === 0) return null;
        return (
          <section key={cat} className="panel">
            <h2>{CATEGORY_LABEL[cat]}</h2>
            <ul className="scenario-list">
              {list.map((s) => (
                <li key={s.id}>
                  <a href={`#/${s.id}`} data-testid={`scenario-link-${s.id}`}>
                    <strong>{s.title}</strong>
                  </a>
                  <p className="scenario-summary">{s.summary}</p>
                </li>
              ))}
            </ul>
          </section>
        );
      })}
    </div>
  );
}

function groupByCategory(scenarios: Scenario[]): Record<ScenarioCategory, Scenario[]> {
  const out: Record<ScenarioCategory, Scenario[]> = {
    reactivity: [],
    filters: [],
    joins: [],
    aggregates: [],
    ordering: [],
    batching: [],
    lifecycle: [],
  };
  for (const s of scenarios) out[s.category].push(s);
  return out;
}
