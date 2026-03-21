import { createStore } from "@xstate/store";
import { z } from "zod";
import { Table, WasmDb } from "./wasmdb.ts";

// --- Helpers ---

const RUNS = 20;

function bench(name: string, fn: () => void): number {
  // warmup 3x
  fn(); fn(); fn();

  const times: number[] = [];
  for (let i = 0; i < RUNS; i++) {
    const start = performance.now();
    fn();
    times.push(performance.now() - start);
  }
  times.sort((a, b) => a - b);
  const avg = times.reduce((a, b) => a + b, 0) / times.length;
  const median = times[Math.floor(times.length / 2)];
  const min = times[0];
  const max = times[times.length - 1];
  const p95 = times[Math.floor(times.length * 0.95)];
  log(`  ${name.padEnd(45)} avg=${avg.toFixed(3).padStart(10)}ms  median=${median.toFixed(3).padStart(10)}ms  min=${min.toFixed(3).padStart(10)}ms  max=${max.toFixed(3).padStart(10)}ms  p95=${p95.toFixed(3).padStart(10)}ms`);
  return avg;
}

function log(msg: string) {
  const el = document.getElementById("root")!;
  el.innerHTML += msg + "\n";
  console.log(msg);
}

// --- xstate/store benchmarks ---

function runXState(n: number) {
  log(`\n  @xstate/store (N=${n}):`);

  bench(`insert (immutable spread)`, () => {
    const store = createStore({
      context: { users: {} as Record<string, { name: string; role: string }> },
      on: {
        addUser: (ctx, event: { id: string; name: string; role: string }) => ({
          ...ctx,
          users: { ...ctx.users, [event.id]: { name: event.name, role: event.role } },
        }),
      },
    });
    for (let i = 0; i < n; i++) {
      store.send({ type: "addUser", id: String(i), name: `User${i}`, role: i % 3 === 0 ? "admin" : "viewer" });
    }
  });

  bench(`insert (mutate)`, () => {
    const store = createStore({
      context: { users: {} as Record<string, { name: string; role: string }> },
      on: {
        addUser: (ctx, event: { id: string; name: string; role: string }) => {
          ctx.users[event.id] = { name: event.name, role: event.role };
          return ctx;
        },
      },
    });
    for (let i = 0; i < n; i++) {
      store.send({ type: "addUser", id: String(i), name: `User${i}`, role: i % 3 === 0 ? "admin" : "viewer" });
    }
  });

  bench(`insert (mutate) + subscribe`, () => {
    let callCount = 0;
    const store = createStore({
      context: { users: {} as Record<string, { name: string; role: string }> },
      on: {
        addUser: (ctx, event: { id: string; name: string; role: string }) => {
          ctx.users[event.id] = { name: event.name, role: event.role };
          return ctx;
        },
      },
    });
    store.subscribe(() => { callCount++; });
    for (let i = 0; i < n; i++) {
      store.send({ type: "addUser", id: String(i), name: `User${i}`, role: i % 3 === 0 ? "admin" : "viewer" });
    }
  });

  bench(`update (mutate)`, () => {
    const store = createStore({
      context: { users: {} as Record<string, { name: string; role: string }> },
      on: {
        addUser: (ctx, event: { id: string; name: string; role: string }) => {
          ctx.users[event.id] = { name: event.name, role: event.role };
          return ctx;
        },
      },
    });
    for (let i = 0; i < n; i++) {
      store.send({ type: "addUser", id: String(i), name: `User${i}`, role: "viewer" });
    }
    for (let i = 0; i < n; i++) {
      store.send({ type: "addUser", id: String(i), name: `User${i}_updated`, role: "admin" });
    }
  });
}

// --- wasmdb benchmarks ---

const usersTable = new Table(
  "users",
  z.object({ name: z.string(), role: z.string() }),
);

const rawTable = new Table("users_raw", z.any());

function newDb(): WasmDb {
  const db = new WasmDb();
  db.reset();
  return db;
}

function runAddBreakdown(n: number) {
  log(`\n  add() breakdown (N=${n}):`);
  const encoder = new TextEncoder();

  bench(`JSON.stringify only`, () => {
    for (let i = 0; i < n; i++) {
      JSON.stringify(["users", String(i), { name: `User${i}`, role: i % 3 === 0 ? "admin" : "viewer" }]);
    }
  });

  bench(`JSON.stringify + encode (alloc)`, () => {
    for (let i = 0; i < n; i++) {
      encoder.encode(JSON.stringify(["users", String(i), { name: `User${i}`, role: i % 3 === 0 ? "admin" : "viewer" }]));
    }
  });

  const encodeBuffer = new Uint8Array(1024 * 1024);
  bench(`JSON.stringify + encodeInto (no alloc)`, () => {
    for (let i = 0; i < n; i++) {
      const json = JSON.stringify(["users", String(i), { name: `User${i}`, role: i % 3 === 0 ? "admin" : "viewer" }]);
      encoder.encodeInto(json, encodeBuffer);
    }
  });

  bench(`object creation only`, () => {
    for (let i = 0; i < n; i++) {
      const _obj = { name: `User${i}`, role: i % 3 === 0 ? "admin" : "viewer" };
    }
  });

  bench(`String(i) + template literals`, () => {
    for (let i = 0; i < n; i++) {
      const _id = String(i);
      const _name = `User${i}`;
      const _role = i % 3 === 0 ? "admin" : "viewer";
    }
  });
}

function runWasmDb(n: number) {
  log(`\n  wasmdb (N=${n}):`);

  bench(`add() only`, () => {
    const db = newDb();
    for (let i = 0; i < n; i++) {
      db.add(usersTable, String(i), { name: `User${i}`, role: i % 3 === 0 ? "admin" : "viewer" });
    }
  });

  bench(`sync() only (after ${n} adds)`, () => {
    const db = newDb();
    for (let i = 0; i < n; i++) {
      db.add(usersTable, String(i), { name: `User${i}`, role: i % 3 === 0 ? "admin" : "viewer" });
    }
    db.sync();
  });

  bench(`insert (add + sync)`, () => {
    const db = newDb();
    for (let i = 0; i < n; i++) {
      db.add(usersTable, String(i), { name: `User${i}`, role: i % 3 === 0 ? "admin" : "viewer" });
    }
    db.sync();
  });

  bench(`insert + projection`, () => {
    const db = newDb();
    let callCount = 0;
    const projId = db.registerProjection(
      {
        table: usersTable,
        query: { bool: { must: [{ term: { role: "admin" } }] } },
        fields: ["_id", "name", "role"] as const,
      },
      () => { callCount++; },
    );
    for (let i = 0; i < n; i++) {
      db.add(usersTable, String(i), { name: `User${i}`, role: i % 3 === 0 ? "admin" : "viewer" });
    }
    db.sync();
    db.unregisterProjection(projId);
  });

  bench(`update`, () => {
    const db = newDb();
    for (let i = 0; i < n; i++) {
      db.add(usersTable, String(i), { name: `User${i}`, role: "viewer" });
    }
    db.sync();
    for (let i = 0; i < n; i++) {
      db.add(usersTable, String(i), { name: `User${i}_updated`, role: "admin" });
    }
    db.sync();
  });
}

// --- Run ---

async function main() {
  const el = document.getElementById("root")!;
  el.style.fontFamily = "monospace";
  el.style.whiteSpace = "pre";
  el.style.padding = "32px";
  el.style.fontSize = "13px";

  log(`=== wasmdb vs @xstate/store benchmark (${RUNS} runs each) ===`);

  for (const n of [1_000, 10_000]) {
    log(`\n${"=".repeat(120)}`);
    log(`N = ${n}`);
    log("=".repeat(120));

    runXState(n);
    runAddBreakdown(n);
    runWasmDb(n);
  }

  log("\ndone.");
}

main();
