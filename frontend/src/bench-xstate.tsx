import { memo, useState } from "react";
import { createRoot } from "react-dom/client";
import { createStore } from "@xstate/store";
import { useSelector } from "@xstate/store/react";
import { renderNav } from "./bench-nav.ts";

const N = 4_000;
const UPDATES = 1;

renderNav("xstate");

const store = createStore({
  context: { users: {} as Record<string, { name: string; role: string }> },
  on: {
    set: (ctx, e: { id: string; name: string; role: string }) => ({
      ...ctx,
      users: { ...ctx.users, [e.id]: { name: e.name, role: e.role } },
    }),
  },
});

let hookTotal = 0;
let renders = 0;

const Child = memo(({ id }: { id: string }) => {
  const t = performance.now();
  const u = useSelector(store, (s) => s.context.users[id]);
  hookTotal += performance.now() - t;
  renders++;
  return <div>{u?.name}</div>;
});

const ids = Array.from({ length: N }, (_, i) => String(i));

function App() {
  const [log, setLog] = useState<string[]>([]);

  return (
    <div style={{ fontFamily: "monospace", padding: 32 }}>
      <h3>xstate/store — {N} components, {UPDATES} update(s)</h3>
      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        <button onClick={() => {
          const t0 = performance.now();
          renders = 0; hookTotal = 0;
          for (let i = 0; i < UPDATES; i++) store.send({ type: "set", id: String(i), name: `X${i}_${Date.now()}`, role: "a" });
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
for (let i = 0; i < N; i++) store.send({ type: "set", id: String(i), name: `User${i}`, role: "v" });
logEl.textContent += `seed ${N}: ${(performance.now() - t0).toFixed(1)}ms\nmounting...\n`;

renders = 0; hookTotal = 0;
const tMount = performance.now();
createRoot(logEl).render(<App />);

const poll = setInterval(() => {
  if (renders >= N) {
    clearInterval(poll);
    console.log(`mount: ${(performance.now() - tMount).toFixed(1)}ms  useSelector(${renders}x): ${hookTotal.toFixed(1)}ms (avg ${(hookTotal / renders).toFixed(3)}ms)`);
  }
}, 100);
