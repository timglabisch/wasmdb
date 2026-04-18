/**
 * Reactive-SQL builders.
 *
 * The reactive engine requires fully-qualified column references
 * (`table.column`) in SELECT projections, WHERE clauses, and ORDER BY.
 * These helpers auto-qualify plain identifiers so call sites stay terse.
 */

const asInt = (n: number): string => {
  if (!Number.isFinite(n) || !Number.isInteger(n)) {
    throw new Error(`queries.ts: id must be a finite integer, got ${n}`);
  }
  return String(n);
};

/** Return true if `expr` is a bare identifier we should qualify. */
const isBareIdent = (expr: string): boolean =>
  /^[A-Za-z_][A-Za-z0-9_]*$/.test(expr);

/** Split a comma-separated list of projections, qualifying bare columns. */
export const qualifyCols = (table: string, cols: string): string =>
  cols
    .split(',')
    .map(c => c.trim())
    .map(c => isBareIdent(c) ? `${table}.${c}` : c)
    .join(', ');

/** Split an ORDER BY list, qualifying bare leading identifiers. `id DESC, x` -> `t.id DESC, t.x` */
export const qualifyOrderBy = (table: string, orderBy: string): string =>
  orderBy
    .split(',')
    .map(part => part.trim())
    .map(part => {
      const m = part.match(/^([A-Za-z_][A-Za-z0-9_]*)(\s+(ASC|DESC))?$/i);
      if (!m) return part;
      const dir = m[2] ?? '';
      return `${table}.${m[1]}${dir}`;
    })
    .join(', ');

/** `SELECT {qualified cols} FROM {table} WHERE {table}.id = {id}` */
export const selectById = (table: string, cols: string, id: number): string =>
  `SELECT ${qualifyCols(table, cols)} FROM ${table} WHERE ${table}.id = ${asInt(id)}`;

/** `SELECT {qualified cols} FROM {table} WHERE {table}.{fkCol} = {val} [ORDER BY qualified]` */
export const selectByFk = (
  table: string,
  cols: string,
  fkCol: string,
  fkVal: number,
  orderBy?: string,
): string => {
  const base = `SELECT ${qualifyCols(table, cols)} FROM ${table} WHERE ${table}.${fkCol} = ${asInt(fkVal)}`;
  return orderBy ? `${base} ORDER BY ${qualifyOrderBy(table, orderBy)}` : base;
};
