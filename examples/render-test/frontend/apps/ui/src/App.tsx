import { useEffect, useState } from 'react';
import { useWasm } from '@wasmdb/client';
import { Playground } from '@wasmdb/playground';
import { ScenarioApp, useRenderCount } from '@wasmdb/scenarios';
import { SCENARIOS } from './scenarios/registry';
import { PLAYGROUND_CONFIG } from './playground/config';
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

  if (route === 'playground') {
    return (
      <main data-testid="app-ready" className="app" data-route="playground">
        <Playground config={PLAYGROUND_CONFIG} />
      </main>
    );
  }

  return (
    <main
      data-testid="app-ready"
      className="app"
      {...(route !== null ? { 'data-scenario-id': route } : {})}
    >
      <ScenarioApp
        scenarios={SCENARIOS}
        title="render-test scenarios"
        playground={{ href: '#/playground' }}
      />
    </main>
  );
}
