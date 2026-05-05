import { useState } from 'react';
import type { Scenario } from './types';
import { CATEGORY_LABEL } from './types';
import { resetRenderLog } from '../test-utils/useRenderCount';
import { ActionProvider, LastActionPanel } from './ActionTracker';
import { SCENARIOS } from './registry';

interface Props {
  scenario: Scenario;
}

/**
 * Common shell for every scenario page. Renders the scenario header
 * (title + summary), a hidden-by-default expectations panel
 * ("predict-then-reveal"), an optional subscriptions reference, the live
 * diff panel, and finally the scenario body. Acts as living documentation
 * with immediate cause→effect feedback.
 */
export function ScenarioLayout({ scenario }: Props) {
  const [revealed, setRevealed] = useState(false);
  const idx = SCENARIOS.findIndex((s) => s.id === scenario.id);
  const prev = idx > 0 ? SCENARIOS[idx - 1] : null;
  const next = idx >= 0 && idx < SCENARIOS.length - 1 ? SCENARIOS[idx + 1] : null;

  return (
    <ActionProvider scenario={scenario}>
      <div className="scenario-page">
        <nav className="scenario-nav">
          <a href="#/" data-testid="scenario-back">← all scenarios</a>
          <span className="scenario-crumb">{CATEGORY_LABEL[scenario.category]}</span>
          <span className="scenario-pos">{idx + 1} / {SCENARIOS.length}</span>
          <span className="scenario-nav-spacer" />
          {prev && (
            <a className="scenario-prev" href={`#/${prev.id}`} title={prev.title}>
              ← prev
            </a>
          )}
          {next && (
            <a className="scenario-next" href={`#/${next.id}`} title={next.title}>
              next →
            </a>
          )}
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
          <header>
            <h2>Predict the outcome</h2>
            <button
              className="reveal-btn"
              onClick={() => setRevealed((r) => !r)}
              data-testid="btn-toggle-expectations"
            >
              {revealed ? 'Hide answer' : 'Reveal expected outcome'}
            </button>
          </header>
          {!revealed ? (
            <p className="scenario-hint">
              Try it first — click a button below and watch which components
              flash yellow. The live diff on the right shows what ticked.
              Then reveal the expected outcome and compare.
            </p>
          ) : (
            <ul>
              {scenario.expectations.map((line, i) => (
                <li key={i}>{line}</li>
              ))}
            </ul>
          )}
        </section>

        <div className="scenario-grid">
          <div className="scenario-body">
            <scenario.Body />
          </div>
          <LastActionPanel />
        </div>

        {scenario.subscriptions && scenario.subscriptions.length > 0 && (
          <section className="scenario-subscriptions">
            <h2>What each component subscribes to</h2>
            <ul>
              {scenario.subscriptions.map((s, i) => (
                <li key={i}>
                  <div className="sub-name"><strong>{s.component}</strong></div>
                  <pre><code>{s.sql}</code></pre>
                  {s.note && <small>{s.note}</small>}
                </li>
              ))}
            </ul>
          </section>
        )}
      </div>
    </ActionProvider>
  );
}
