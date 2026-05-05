import { scenariosByCategory } from './registry';
import { CATEGORY_LABEL, type ScenarioCategory } from './types';

const ORDER: ScenarioCategory[] = ['counters', 'users', 'rooms', 'messages', 'joins', 'hooks'];

/**
 * Landing page. Lists every scenario grouped by category. Each entry links
 * to its self-contained page where the test can be reproduced manually.
 */
export function ScenarioIndex() {
  const grouped = scenariosByCategory();
  return (
    <div className="scenario-index">
      <header>
        <h1>render-test scenarios</h1>
        <p>
          Each link opens a self-contained page mirroring one Playwright
          spec. Components flash <span className="hint-flash">yellow</span>
          when they re-render, and a <strong>live diff panel</strong> shows
          what ticked after each click — marked ✓ if expected, ✗ if it
          broke isolation. Try predicting first, click, watch, then reveal
          the answer.
        </p>
        <p className="scenario-index-playground">
          Or jump into the <a href="#/playground" data-testid="playground-link"><strong>data playground</strong></a> —
          live tables for every domain table, inline editing, same in-memory
          database the scenarios use.
        </p>
      </header>
      {ORDER.map((cat) => (
        <section key={cat} className="panel">
          <h2>{CATEGORY_LABEL[cat]}</h2>
          <ul className="scenario-list">
            {grouped[cat].map((s) => (
              <li key={s.id}>
                <a href={`#/${s.id}`} data-testid={`scenario-link-${s.id}`}>
                  <strong>{s.title}</strong>
                </a>
                <p className="scenario-summary">{s.summary}</p>
              </li>
            ))}
          </ul>
        </section>
      ))}
    </div>
  );
}
