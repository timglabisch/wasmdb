import { peekQuery } from '@/wasm';
import { selectById } from '@/queries';
import type { ProductRow } from '../types';

const COLS =
  'sku, name, description, unit, unit_price, tax_rate, cost_price, active';

const rowToProduct = (r: any[]): ProductRow => ({
  sku: r[0] as string,
  name: r[1] as string,
  description: r[2] as string,
  unit: r[3] as string,
  unit_price: r[4] as number,
  tax_rate: r[5] as number,
  cost_price: r[6] as number,
  active: r[7] as number,
});

/** One-shot non-reactive full-row read. Used at write time to compose UpdateProduct payloads. */
export function peekProduct(productId: number): ProductRow | null {
  const rows = peekQuery(selectById('products', COLS, productId));
  if (rows.length === 0) return null;
  return rowToProduct(rows[0]);
}
