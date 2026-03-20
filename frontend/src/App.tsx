import { z } from "zod";
import { Table, db, useProjection } from "./wasmdb.ts";

const usersTable = new Table(
  "users",
  z.object({ name: z.string(), role: z.string() }),
);

const productsTable = new Table(
  "products",
  z.object({ title: z.string(), price: z.string() }),
);

export function App() {
  const admins = useProjection({
    table: usersTable,
    query: {
      bool: {
        must: [{ term: { role: "admin" } }],
      },
    },
    fields: ["_id", "name", "role"] as const,
  });

  function addSampleData() {
    db.add(usersTable, "1", { name: "Alice", role: "admin" });
    db.add(usersTable, "2", { name: "Bob", role: "viewer" });
    db.add(usersTable, "3", { name: "Charlie", role: "admin" });
    db.add(productsTable, "a", { title: "Widget", price: "9.99" });
    db.sync();
  }

  function updateAlice() {
    db.add(usersTable, "1", { name: "Alice", role: "superadmin" });
    db.sync();
  }

  return (
    <div style={{ fontFamily: "monospace", padding: 32 }}>
      <h1>wasmdb</h1>
      <div style={{ display: "flex", gap: 8 }}>
        <button onClick={addSampleData}>add sample data</button>
        <button onClick={updateAlice}>update alice &rarr; superadmin</button>
      </div>
      <h2>projection: admins (role=admin)</h2>
      <pre>{JSON.stringify(admins, null, 2)}</pre>
    </div>
  );
}
