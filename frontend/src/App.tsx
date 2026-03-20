import { useEffect, useRef, useState } from "react";
import { z } from "zod";
import { WasmDb, Table, ProjectionData } from "./wasmdb.ts";

const usersTable = new Table(
  "users",
  z.object({ name: z.string(), role: z.string() }),
);

const productsTable = new Table(
  "products",
  z.object({ title: z.string(), price: z.string() }),
);

type UserWithId = z.infer<typeof usersTable.schema> & { _id: string };

const db = new WasmDb();

export function App() {
  const [tables, setTables] = useState<
    Record<string, Record<string, Record<string, string>>>
  >({});
  const [admins, setAdmins] = useState<ProjectionData<UserWithId>>({});
  const projIdRef = useRef<number | null>(null);

  useEffect(() => {
    projIdRef.current = db.registerProjection(
      {
        table: usersTable,
        query: {
          bool: {
            must: [{ term: { role: "admin" } }],
          },
        },
        fields: ["_id", "name", "role"] as const,
      },
      setAdmins,
    );

    return () => {
      if (projIdRef.current !== null) {
        db.unregisterProjection(projIdRef.current);
      }
    };
  }, []);

  function addSampleData() {
    db.add(usersTable, "1", { name: "Alice", role: "admin" });
    db.add(usersTable, "2", { name: "Bob", role: "viewer" });
    db.add(usersTable, "3", { name: "Charlie", role: "admin" });
    db.add(productsTable, "a", { title: "Widget", price: "9.99" });
    setTables(db.sync());
  }

  function updateAlice() {
    db.add(usersTable, "1", { name: "Alice", role: "superadmin" });
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
          update alice &rarr; superadmin
        </button>
      </div>
      <h2>all data</h2>
      <pre>{JSON.stringify(tables, null, 2)}</pre>
      <h2>projection: admins (role=admin)</h2>
      <pre>{JSON.stringify(admins, null, 2)}</pre>
    </div>
  );
}
