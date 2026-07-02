/**
 * Build a hash-route href for jumping into the playground with optional
 * deep-link state. Pure helper, no DOM access.
 *
 * URL contract (agreed with the playground package):
 *   - `#/playground` — plain
 *   - `#/playground?from=<scenario-id>` — back-link returns to that scenario
 *   - `#/playground?from=<scenario-id>&sql=<URI-encoded SQL>` — pre-opens a query tab
 *   - `#/playground?from=<scenario-id>&table=<table-name>` — pre-opens that table tab
 *
 * Both `sql` and `table` may be present together.
 */
export function buildPlaygroundHref(opts: {
  base?: string;
  from?: string;
  sql?: string;
  table?: string;
}): string {
  const base = opts.base ?? '#/playground';
  const params = new URLSearchParams();
  if (opts.from) params.set('from', opts.from);
  if (opts.sql) params.set('sql', opts.sql);
  if (opts.table) params.set('table', opts.table);
  const qs = params.toString();
  return qs.length === 0 ? base : `${base}?${qs}`;
}
