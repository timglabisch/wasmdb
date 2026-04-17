import { useWasm } from './sync.ts';
import StatsBar from './StatsBar.tsx';
import UsersPanel from './UsersPanel.tsx';
import OrdersPanel from './OrdersPanel.tsx';
import BulkActions from './BulkActions.tsx';
import { DebugToolbar } from './debug';
import './index.css';

export default function App() {
  const ready = useWasm();

  if (!ready) return <div className="loading">loading wasm...</div>;

  return (
    <div className="app">
      <h1>wasmdb sync</h1>
      <p className="subtitle">
        optimistic client (WASM) + authoritative server (Axum) — borsh protocol
      </p>
      <StatsBar />
      <BulkActions />
      <div className="dashboard-grid">
        <UsersPanel />
        <OrdersPanel />
      </div>
      {import.meta.env.DEV && <DebugToolbar />}
    </div>
  );
}
