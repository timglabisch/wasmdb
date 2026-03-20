import { useEffect, useRef, useState } from "react";
import { WasmDb, ProjectionData } from "./wasmdb.ts";

type Row = Record<string, string>;
type Tables = Record<string, Record<string, Row>>;

export function App() {
  const [db, setDb] = useState<WasmDb | null>(null);
  const [tables, setTables] = useState<Tables>({});
  const [admins, setAdmins] = useState<ProjectionData>({});
  const projIdRef = useRef<number | null>(null);

  useEffect(() => {
    WasmDb.init().then((db) => {
      projIdRef.current = db.registerProjection({
        query: {
          bool: {
            must: [
              { term: { _table: "users" } },
              { term: { role: "admin" } },
            ],
          },
        },
        fields: ["_id", "name", "role"],
      }, setAdmins);
      setDb(db);
    });

    return () => {
      if (db && projIdRef.current !== null) {
        db.unregisterProjection(projIdRef.current);
      }
    };
  }, []);

  function addSampleData() {
    if (!db) return;
    db.add("users", "1", { name: "Alice", role: "admin" });
    db.add("users", "2", { name: "Bob", role: "viewer" });
    db.add("users", "3", { name: "Charlie", role: "admin" });
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
      <h2>all data</h2>
      <pre>{JSON.stringify(tables, null, 2)}</pre>
      <h2>projection: admins (role=admin)</h2>
      <pre>{JSON.stringify(admins, null, 2)}</pre>
    </div>
  );
}
