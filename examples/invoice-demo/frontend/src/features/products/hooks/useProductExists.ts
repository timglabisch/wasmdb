import { useQuery } from '@/wasm';

/**
 * Reactive existence check. Using a single-column subscription instead of the
 * full-row subscription means the detail shell only re-renders when the row
 * appears/disappears — not on every field edit.
 */
export function useProductExists(productId: number): boolean {
  const rows = useQuery(
    `SELECT products.id FROM products WHERE products.id = ${productId}`,
    ([id]) => id as number,
  );
  return rows.length > 0;
}
