import { useEffect, useState } from 'react';
import type { Scenario } from './types';
import { ScenarioIndex, type PlaygroundLink } from './ScenarioIndex';
import { ScenarioLayout } from './ScenarioLayout';

interface Props {
  scenarios: Scenario[];
  playground?: PlaygroundLink;
  /** Header title on the index page. Defaults to "Scenarios". */
  title?: string;
  /** Custom intro paragraph on the index. Falls back to a generic blurb. */
  intro?: React.ReactNode;
}

function parseHash(): string | null {
  const h = window.location.hash;
  if (!h || h === '#' || h === '#/') return null;
  return h.replace(/^#\/?/, '');
}

function useHashRoute(): string | null {
  const [route, setRoute] = useState<string | null>(parseHash());
  useEffect(() => {
    const onHash = () => setRoute(parseHash());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);
  return route;
}

/**
 * Drop-in scenario harness with hash-based routing. Renders:
 *   - `ScenarioIndex` when the hash is empty (`#` or `#/`)
 *   - `ScenarioLayout` when the hash matches a scenario id
 *   - A "scenario not found" fallback otherwise
 *
 * The consumer should still own the top-level `<main>`/wrapper element so
 * any styling around the scenario stays under the consumer's control.
 */
export function ScenarioApp({ scenarios, playground, title, intro }: Props) {
  const route = useHashRoute();

  if (route === null) {
    return <ScenarioIndex scenarios={scenarios} playground={playground} title={title} intro={intro} />;
  }

  const scenario = scenarios.find((s) => s.id === route);
  if (!scenario) {
    return (
      <div className="scenario-page">
        <nav className="scenario-nav">
          <a href="#/" data-testid="scenario-back">← all scenarios</a>
        </nav>
        <h1>Unknown scenario: <code>{route}</code></h1>
        <p>Pick one from the index.</p>
      </div>
    );
  }

  return <ScenarioLayout scenario={scenario} scenarios={scenarios} />;
}
