import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    // All confirm-server routes must reach :3126 in dev (the UI runs on
    // vite :5173). `/command` confirms, `/fetch` backfills gap ancestors,
    // `/heads` seeds the bootstrap, `/foreign-write` injects another writer.
    // Missing any of these makes the call hit vite and 404 (a failed
    // `bootstrap` on load looks exactly like "reload wiped everything").
    proxy: {
      '/command': 'http://localhost:3126',
      '/fetch': 'http://localhost:3126',
      '/heads': 'http://localhost:3126',
      '/foreign-write': 'http://localhost:3126',
    },
  },
});
