import { useEffect, useState } from 'react';
import { useWasm } from '@wasmdb/client';
import { CounterPanel } from './components/CounterPanel';
import { MessageList } from './components/MessageList';
import { MessageCount } from './components/MessageCount';
import { RoomList } from './components/RoomList';
import { RoomWithOwnerName } from './components/RoomWithOwnerName';
import { RoomsWithMessages } from './components/RoomsWithMessages';
import { OnlineUserList } from './components/OnlineUserList';
import { ScenarioControls } from './components/ScenarioControls';
import { PeekProbe } from './components/PeekProbe';
import { IdSwapProbe } from './components/IdSwapProbe';
import { UnmountProbe } from './components/UnmountProbe';
import { useRenderCount } from './test-utils/useRenderCount';
import { SEED, seed } from './seed';
import './index.css';

export default function App() {
  const ready = useWasm();
  const [seeded, setSeeded] = useState(false);
  useRenderCount('App');

  useEffect(() => {
    if (!ready || seeded) return;
    void seed().then(() => setSeeded(true));
  }, [ready, seeded]);

  if (!ready) return <div data-testid="loading">loading wasm…</div>;
  if (!seeded) return <div data-testid="seeding">seeding…</div>;

  return (
    <main data-testid="app-ready" className="app">
      <header>
        <h1>render-test</h1>
        <p>
          Re-render integration test. Each component shows its render count.
          Trigger commands and verify only the expected components re-render.
        </p>
      </header>
      <ScenarioControls />
      <CounterPanel
        ids={[SEED.counters.C1, SEED.counters.C2, SEED.counters.C3, SEED.counters.C4]}
      />
      <RoomList />
      <OnlineUserList />
      <section className="panel">
        <h2>Room joins</h2>
        <RoomWithOwnerName roomId={SEED.rooms.R1} />
        <RoomWithOwnerName roomId={SEED.rooms.R2} />
        <RoomWithOwnerName roomId={SEED.rooms.R3} />
      </section>
      <RoomsWithMessages />
      <section className="panel">
        <h2>Messages by room</h2>
        <div className="message-grid">
          <MessageList roomId={SEED.rooms.R1} />
          <MessageList roomId={SEED.rooms.R2} />
          <MessageList roomId={SEED.rooms.R3} />
        </div>
        <div>
          <span>R1: <MessageCount roomId={SEED.rooms.R1} /></span>
          <span>R2: <MessageCount roomId={SEED.rooms.R2} /></span>
          <span>R3: <MessageCount roomId={SEED.rooms.R3} /></span>
        </div>
      </section>
      <section className="panel">
        <h2>Hook probes</h2>
        <PeekProbe />
        <IdSwapProbe />
        <UnmountProbe />
      </section>
    </main>
  );
}
