import { peekQuery } from '@/wasm';
import type { ProductWithoutPk } from '@/generated/tables/Product';

const rowToProduct = (r: any[]): ProductWithoutPk => ({
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
export function peekProduct(productId: string): ProductWithoutPk | null {
  const rows = peekQuery(
    `SELECT products.sku, products.name, products.description, products.unit, ` +
    `products.unit_price, products.tax_rate, products.cost_price, products.active ` +
    `FROM products WHERE products.id = :id`,
    { id: productId },
  );
  if (rows.length === 0) return null;
  return rowToProduct(rows[0]);
}
