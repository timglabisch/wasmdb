import type { ReactNode } from 'react';
import type { Scenario } from './types';
import { CATEGORY_LABEL } from './types';
import { resetRenderLog } from '../test-utils/useRenderCount';

interface Props {
  scenario: Scenario;
  children?: ReactNode;
}

/**
 * Common shell for every scenario page. Renders the explanatory header
 * (title + summary + expectations) followed by the scenario body. Acts as
 * "living documentation": each test maps to one URL with a self-contained
 * UI you can poke at manually to understand the asserted behavior.
 */
export function ScenarioLayout({ scenario }: Props) {
  return (
    <div className="scenario-page">
      <nav className="scenario-nav">
        <a href="#/" data-testid="scenario-back">← all scenarios</a>
        <span className="scenario-crumb">{CATEGORY_LABEL[scenario.category]}</span>
      </nav>

      <header className="scenario-header">
        <h1>{scenario.title}</h1>
        <p className="scenario-summary">{scenario.summary}</p>
        <div className="scenario-meta">
          <code>#/{scenario.id}</code>
          <button
            data-testid="btn-reset-render-log"
            onClick={() => resetRenderLog()}
          >
            Reset render counts
          </button>
        </div>
      </header>

      <section className="scenario-expectations">
        <h2>What to observe</h2>
        <ul>
          {scenario.expectations.map((line, i) => (
            <li key={i}>{line}</li>
          ))}
        </ul>
      </section>

      <section className="scenario-body">
        <scenario.Body />
      </section>
    </div>
  );
}
