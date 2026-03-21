import { memo, useState } from "react";
import { createRoot } from "react-dom/client";
import { createStore } from "@xstate/store";
import { useSelector } from "@xstate/store/react";
import { z } from "zod";
import { Table, db, useProjection } from "./wasmdb.ts";
import { renderNav } from "./bench-nav.ts";

renderNav("both");

const N = 4_000;
const UPDATES = 1;

// --- hook timing ---

let xHookTotal = 0;
let wHookTotal = 0;
let xRenders = 0;
let wRenders = 0;

// --- xstate ---

const store = createStore({
  context: { users: {} as Record<string, { name: string; role: string }> },
  on: {
    set: (ctx, e: { id: string; name: string; role: string }) => ({
      ...ctx,
      users: { ...ctx.users, [e.id]: { name: e.name, role: e.role } },
    }),
  },
});

const XChild = memo(({ id }: { id: string }) => {
  const t = performance.now();
  const u = useSelector(store, (s) => s.context.users[id]);
  xHookTotal += performance.now() - t;
  xRenders++;
  return <div>{u?.name}</div>;
});

// --- wasmdb ---

const table = new Table("users", z.object({ name: z.string(), role: z.string() }));

const WChild = memo(({ id }: { id: string }) => {
  const t = performance.now();
  const data = useProjection({ table, query: { term: { _id: id } }, fields: ["name"] as const });
  wHookTotal += performance.now() - t;
  wRenders++;
  const row = Object.values(data)[0];
  return <div>{row?.name}</div>;
});

// --- app ---

const ids = Array.from({ length: N }, (_, i) => String(i));

function App() {
  const [log, setLog] = useState<string[]>([]);

  return (
    <div style={{ fontFamily: "monospace", padding: 32 }}>
      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        <button onClick={() => {
          const t0 = performance.now();
          xRenders = 0; xHookTotal = 0;
          for (let i = 0; i < UPDATES; i++) store.send({ type: "set", id: String(i), name: `X${i}_${Date.now()}`, role: "a" });
          const syncMs = performance.now() - t0;
          requestAnimationFrame(() => {
            const totalMs = performance.now() - t0;
            setLog(l => [...l, `xstate:  sync=${syncMs.toFixed(1)}ms  total=${totalMs.toFixed(1)}ms  hooks=${xHookTotal.toFixed(1)}ms  re-renders=${xRenders}`]);
          });
        }}>update xstate ({UPDATES})</button>

        <button onClick={() => {
          const t0 = performance.now();
          wRenders = 0; wHookTotal = 0;
          for (let i = 0; i < UPDATES; i++) db.add(table, String(i), { name: `W${i}_${Date.now()}`, role: "a" });
          db.sync();
          const syncMs = performance.now() - t0;
          requestAnimationFrame(() => {
            const totalMs = performance.now() - t0;
            setLog(l => [...l, `wasmdb:  sync=${syncMs.toFixed(1)}ms  total=${totalMs.toFixed(1)}ms  hooks=${wHookTotal.toFixed(1)}ms  re-renders=${wRenders}`]);
          });
        }}>update wasmdb ({UPDATES})</button>

        <button onClick={() => setLog([])}>clear</button>
      </div>
      <pre style={{ marginBottom: 16 }}>{log.join("\n") || "click a button"}</pre>
      <div style={{ display: "flex", gap: 32 }}>
        <div>
          <b>xstate ({N})</b>
          {ids.map((id) => <XChild key={id} id={id} />)}
        </div>
        <div>
          <b>wasmdb ({N})</b>
          {ids.map((id) => <WChild key={id} id={id} />)}
        </div>
      </div>
    </div>
  );
}

// --- boot with timing ---

const logEl = document.getElementById("root")!;
logEl.style.fontFamily = "monospace";
logEl.style.padding = "32px";
logEl.style.whiteSpace = "pre";
logEl.textContent = "booting...\n";

const t0 = performance.now();

for (let i = 0; i < N; i++) {
  store.send({ type: "set", id: String(i), name: `User${i}`, role: "v" });
}
const seedXMs = performance.now() - t0;
logEl.textContent += `xstate seed ${N}: ${seedXMs.toFixed(1)}ms\n`;

const t1 = performance.now();
for (let i = 0; i < N; i++) {
  db.add(table, String(i), { name: `User${i}`, role: "v" });
}
db.sync();
const seedWMs = performance.now() - t1;
logEl.textContent += `wasmdb seed ${N}: ${seedWMs.toFixed(1)}ms\n`;
logEl.textContent += `mounting ${N * 2} components...\n`;

xRenders = 0; xHookTotal = 0;
wRenders = 0; wHookTotal = 0;
const tMount = performance.now();

createRoot(logEl).render(<App />);

// poll until all components have rendered
const mountPoll = setInterval(() => {
  if (xRenders >= N && wRenders >= N) {
    clearInterval(mountPoll);
    const mountMs = performance.now() - tMount;
    console.log(
      `=== mount timing ===\n` +
      `total mount: ${mountMs.toFixed(1)}ms\n` +
      `useSelector hooks (${xRenders}x): ${xHookTotal.toFixed(1)}ms (avg ${(xHookTotal / xRenders).toFixed(3)}ms)\n` +
      `useProjection hooks (${wRenders}x): ${wHookTotal.toFixed(1)}ms (avg ${(wHookTotal / wRenders).toFixed(3)}ms)`
    );
  }
}, 100);
