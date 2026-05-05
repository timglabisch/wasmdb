import { useEffect, useState } from 'react';
import { useWasm } from '@wasmdb/client';
import { Explorer } from './explorer/Explorer';
import { ScenarioIndex } from './scenarios/Index';
import { ScenarioLayout } from './scenarios/Layout';
import { SCENARIOS_BY_ID } from './scenarios/registry';
import { useRenderCount } from './test-utils/useRenderCount';
import { seed } from './seed';
import './index.css';

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

export default function App() {
  const ready = useWasm();
  const [seeded, setSeeded] = useState(false);
  const route = useHashRoute();
  useRenderCount('App');

  useEffect(() => {
    if (!ready || seeded) return;
    void seed().then(() => setSeeded(true));
  }, [ready, seeded]);

  if (!ready) return <div data-testid="loading">loading wasm…</div>;
  if (!seeded) return <div data-testid="seeding">seeding…</div>;

  if (route === null) {
    return (
      <main data-testid="app-ready" className="app">
        <ScenarioIndex />
      </main>
    );
  }

  if (route === 'playground') {
    return (
      <main data-testid="app-ready" className="app" data-route="playground">
        <Explorer />
      </main>
    );
  }

  const scenario = SCENARIOS_BY_ID[route];
  if (!scenario) {
    return (
      <main data-testid="app-ready" className="app">
        <div className="scenario-page">
          <nav className="scenario-nav">
            <a href="#/" data-testid="scenario-back">← all scenarios</a>
          </nav>
          <h1>Unknown scenario: <code>{route}</code></h1>
          <p>Pick one from the index.</p>
        </div>
      </main>
    );
  }

  return (
    <main data-testid="app-ready" className="app" data-scenario-id={scenario.id}>
      <ScenarioLayout scenario={scenario} />
    </main>
  );
}
