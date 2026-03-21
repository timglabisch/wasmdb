import { useState, useEffect, useRef, memo, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { createStore } from "@xstate/store";
import { useSelector } from "@xstate/store/react";
import { z } from "zod";
import { Table, db, useProjection } from "./wasmdb.ts";

// --- Config ---

const NUM_COMPONENTS = 1_000;
const NUM_UPDATES = 100;

// --- Shared table + xstate store ---

const usersTable = new Table(
  "users",
  z.object({ name: z.string(), role: z.string() }),
);

const xstateStore = createStore({
  context: { users: {} as Record<string, { name: string; role: string }> },
  on: {
    setUser: (ctx, event: { id: string; name: string; role: string }) => ({
      ...ctx,
      users: { ...ctx.users, [event.id]: { name: event.name, role: event.role } },
    }),
  },
});

// --- Render-count tracking ---

let renderCounts = new Int32Array(NUM_COMPONENTS);

// --- xstate child component ---

const XStateChild = memo(function XStateChild({ id, idx }: { id: string; idx: number }) {
  const user = useSelector(xstateStore, (s) => s.context.users[id]);
  renderCounts[idx]++;
  return <div data-id={id}>{user?.name ?? "-"}</div>;
});

// --- wasmdb child component ---

const WasmDbChild = memo(function WasmDbChild({ id, idx }: { id: string; idx: number }) {
  const data = useProjection({
    table: usersTable,
    query: { term: { _id: id } },
    fields: ["_id", "name", "role"] as const,
  });
  renderCounts[idx]++;
  const row = Object.values(data)[0];
  return <div data-id={id}>{row?.name ?? "-"}</div>;
});

// --- Helpers ---

function waitForRender(): Promise<void> {
  return new Promise(r => requestAnimationFrame(() => requestAnimationFrame(() => r())));
}

function log(msg: string) {
  const el = document.getElementById("log")!;
  el.textContent += msg + "\n";
  console.log(msg);
}

// --- Benchmark functions (no React overhead for the harness itself) ---

async function runXState() {
  log(`=== xstate/store: ${NUM_COMPONENTS} components, ${NUM_UPDATES} updates ===`);

  // Seed
  for (let i = 0; i < NUM_COMPONENTS; i++) {
    xstateStore.send({ type: "setUser", id: String(i), name: `User${i}`, role: "viewer" });
  }

  // Mount into a separate container
  const container = document.getElementById("bench-mount")!;
  const root = createRoot(container);
  const kids = [];
  for (let i = 0; i < NUM_COMPONENTS; i++) {
    kids.push(<XStateChild key={i} id={String(i)} idx={i} />);
  }
  root.render(<>{kids}</>);
  await waitForRender();
  await new Promise(r => setTimeout(r, 200));
  log(`[mounted ${NUM_COMPONENTS} components]`);

  // Reset render counts
  renderCounts = new Int32Array(NUM_COMPONENTS);

  // Trigger updates one by one, let React process each
  const start = performance.now();
  for (let u = 0; u < NUM_UPDATES; u++) {
    xstateStore.send({ type: "setUser", id: String(u), name: `User${u}_v${u}`, role: "admin" });
  }
  await waitForRender();
  await new Promise(r => setTimeout(r, 50));
  const elapsed = performance.now() - start;

  const totalRenders = renderCounts.reduce((a, b) => a + b, 0);
  const reRendered = renderCounts.filter(c => c > 0).length;
  log(`time:       ${elapsed.toFixed(3)}ms`);
  log(`re-renders: ${totalRenders} total (${reRendered} of ${NUM_COMPONENTS} components re-rendered)`);

  root.unmount();
  container.innerHTML = "";
  log(`done.\n`);
}

async function runWasmDb() {
  log(`=== wasmdb: ${NUM_COMPONENTS} components, ${NUM_UPDATES} updates ===`);

  // Seed
  db.reset();
  for (let i = 0; i < NUM_COMPONENTS; i++) {
    db.add(usersTable, String(i), { name: `User${i}`, role: "viewer" });
  }
  db.sync();

  // Mount into a separate container
  const container = document.getElementById("bench-mount")!;
  const root = createRoot(container);
  const kids = [];
  for (let i = 0; i < NUM_COMPONENTS; i++) {
    kids.push(<WasmDbChild key={i} id={String(i)} idx={i} />);
  }
  root.render(<>{kids}</>);
  await waitForRender();
  await new Promise(r => setTimeout(r, 200));

  // Flush initial registration diffs so they don't pollute the measurement
  db.sync();
  await waitForRender();
  await new Promise(r => setTimeout(r, 200));
  log(`[mounted ${NUM_COMPONENTS} components]`);

  // Reset render counts
  renderCounts = new Int32Array(NUM_COMPONENTS);

  // Trigger updates
  const start = performance.now();
  for (let u = 0; u < NUM_UPDATES; u++) {
    db.add(usersTable, String(u), { name: `User${u}_v${u}`, role: "admin" });
  }
  db.sync();
  await waitForRender();
  await new Promise(r => setTimeout(r, 50));
  const elapsed = performance.now() - start;

  const totalRenders = renderCounts.reduce((a, b) => a + b, 0);
  const reRendered = renderCounts.filter(c => c > 0).length;
  log(`time:       ${elapsed.toFixed(3)}ms`);
  log(`re-renders: ${totalRenders} total (${reRendered} of ${NUM_COMPONENTS} components re-rendered)`);

  root.unmount();
  container.innerHTML = "";
  log(`done.\n`);
}

// --- Static page (no React for the harness) ---

document.getElementById("root")!.innerHTML = `
  <div style="font-family: monospace; padding: 32px;">
    <h2>React Component Benchmark (${NUM_COMPONENTS} children, ${NUM_UPDATES} updates)</h2>
    <div style="display: flex; gap: 8px; margin-bottom: 16px;">
      <button id="btn-xstate">Run xstate/store</button>
      <button id="btn-wasmdb">Run wasmdb</button>
      <button id="btn-both">Run both</button>
    </div>
    <pre id="log" style="font-size: 13px; line-height: 1.5;"></pre>
    <div id="bench-mount" style="display: none;"></div>
  </div>
`;

document.getElementById("btn-xstate")!.onclick = runXState;
document.getElementById("btn-wasmdb")!.onclick = runWasmDb;
document.getElementById("btn-both")!.onclick = async () => { await runXState(); await runWasmDb(); };
