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
          Each link opens a self-contained page that mirrors exactly one
          Playwright spec. The page shows the relevant components, the
          buttons that drive the scenario, and a description of what to
          observe — a living reference for the engine's reactivity
          guarantees.
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
