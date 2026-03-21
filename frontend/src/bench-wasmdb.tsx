import { memo, useState } from "react";
import { createRoot } from "react-dom/client";
import { z } from "zod";
import { Table, db, useProjection } from "./wasmdb.ts";
import { renderNav } from "./bench-nav.ts";

const N = 4_000;
const UPDATES = 1;

renderNav("wasmdb");

const table = new Table("users", z.object({ name: z.string(), role: z.string() }));

let hookTotal = 0;
let renders = 0;

const Child = memo(({ id }: { id: string }) => {
  const t = performance.now();
  const data = useProjection({ table, query: { term: { _id: id } }, fields: ["name"] as const });
  hookTotal += performance.now() - t;
  renders++;
  const row = Object.values(data)[0];
  return <div>{row?.name}</div>;
});

const ids = Array.from({ length: N }, (_, i) => String(i));

function App() {
  const [log, setLog] = useState<string[]>([]);

  return (
    <div style={{ fontFamily: "monospace", padding: 32 }}>
      <h3>wasmdb — {N} components, {UPDATES} update(s)</h3>
      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        <button onClick={() => {
          const t0 = performance.now();
          renders = 0; hookTotal = 0;
          for (let i = 0; i < UPDATES; i++) db.add(table, String(i), { name: `W${i}_${Date.now()}`, role: "a" });
          db.sync();
          const syncMs = performance.now() - t0;
          requestAnimationFrame(() => {
            const totalMs = performance.now() - t0;
            setLog(l => [...l, `sync=${syncMs.toFixed(1)}ms  total=${totalMs.toFixed(1)}ms  hooks=${hookTotal.toFixed(1)}ms  re-renders=${renders}`]);
          });
        }}>update ({UPDATES})</button>
        <button onClick={() => setLog([])}>clear</button>
      </div>
      <pre style={{ marginBottom: 16 }}>{log.join("\n") || "click update"}</pre>
      {ids.map((id) => <Child key={id} id={id} />)}
    </div>
  );
}

// seed
const logEl = document.getElementById("root")!;
logEl.style.cssText = "font-family:monospace;padding:32px;white-space:pre;";
logEl.textContent = "seeding...\n";

const t0 = performance.now();
for (let i = 0; i < N; i++) db.add(table, String(i), { name: `User${i}`, role: "v" });
db.sync();
logEl.textContent += `seed ${N}: ${(performance.now() - t0).toFixed(1)}ms\nmounting...\n`;

renders = 0; hookTotal = 0;
const tMount = performance.now();
createRoot(logEl).render(<App />);

const poll = setInterval(() => {
  if (renders >= N) {
    clearInterval(poll);
    const mountMs = performance.now() - tMount;
    console.log(`mount: ${mountMs.toFixed(1)}ms  useProjection(${renders}x): ${hookTotal.toFixed(1)}ms (avg ${(hookTotal / renders).toFixed(3)}ms)`);
    // flush initial registration diffs so components get their data
    db.sync();
    console.log(`initial sync after mount: ${(performance.now() - tMount - mountMs).toFixed(1)}ms`);
  }
}, 100);
