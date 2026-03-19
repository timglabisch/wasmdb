import { useEffect, useState } from "react";
import { WasmDb } from "./wasmdb.ts";

type Tables = Record<string, Record<string, Record<string, string>>>;

export function App() {
  const [db, setDb] = useState<WasmDb | null>(null);
  const [tables, setTables] = useState<Tables>({});

  useEffect(() => {
    WasmDb.init().then(setDb);
  }, []);

  function addSampleData() {
    if (!db) return;

    db.add("users", "1", { name: "Alice", role: "admin" });
    db.add("users", "2", { name: "Bob", role: "viewer" });
    db.add("products", "a", { title: "Widget", price: "9.99" });
    setTables(db.sync());
  }

  function updateAlice() {
    if (!db) return;

    db.add("users", "1", { name: "Alice", role: "superadmin" });
    setTables(db.sync());
  }

  return (
    <div style={{ fontFamily: "monospace", padding: 32 }}>
      <h1>wasmdb</h1>
      <div style={{ display: "flex", gap: 8 }}>
        <button onClick={addSampleData} disabled={!db}>
          add sample data
        </button>
        <button onClick={updateAlice} disabled={!db}>
          update alice → superadmin
        </button>
      </div>
      <pre>{JSON.stringify(tables, null, 2)}</pre>
    </div>
  );
}
