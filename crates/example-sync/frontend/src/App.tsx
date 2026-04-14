import { useWasm } from './sync.ts';
import AddUserForm from './AddUserForm.tsx';
import UsersTable from './UsersTable.tsx';
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
      <AddUserForm />
      <UsersTable />
    </div>
  );
}
