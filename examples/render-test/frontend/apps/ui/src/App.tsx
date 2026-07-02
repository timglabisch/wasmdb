import { useEffect, useState } from 'react';
import { useWasm } from '@wasmdb/client';
import { Playground } from '@wasmdb/playground';
import { ScenarioApp, useRenderCount } from '@wasmdb/scenarios';
import { SCENARIOS } from './scenarios/registry';
import { PLAYGROUND_CONFIG } from './playground/config';
import { seed } from './seed';
import './index.css';

interface HashRoute {
  path: string | null;
  params: URLSearchParams;
}

function parseHash(): HashRoute {
  const h = window.location.hash;
  if (!h || h === '#' || h === '#/') return { path: null, params: new URLSearchParams() };
  const stripped = h.replace(/^#\/?/, '');
  const qIdx = stripped.indexOf('?');
  if (qIdx === -1) return { path: stripped, params: new URLSearchParams() };
  const path = stripped.slice(0, qIdx);
  const params = new URLSearchParams(stripped.slice(qIdx + 1));
  return { path: path === '' ? null : path, params };
}

function useHashRoute(): HashRoute {
  const [route, setRoute] = useState<HashRoute>(parseHash());
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

  if (route.path === 'playground') {
    const from = route.params.get('from');
    const sql = route.params.get('sql');
    const table = route.params.get('table');
    const config = from
      ? { ...PLAYGROUND_CONFIG, backHref: `#/${from}`, backLabel: '← back to scenario' }
      : PLAYGROUND_CONFIG;
    return (
      <main data-testid="app-ready" className="app" data-route="playground">
        <Playground
          config={config}
          initialSql={sql ?? undefined}
          initialTable={table ?? undefined}
        />
      </main>
    );
  }

  return (
    <main
      data-testid="app-ready"
      className="app"
      {...(route.path !== null ? { 'data-scenario-id': route.path } : {})}
    >
      <ScenarioApp
        scenarios={SCENARIOS}
        title="render-test scenarios"
        playground={{ href: '#/playground' }}
      />
    </main>
  );
}
