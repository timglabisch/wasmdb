import { useQuery } from '@/wasm';
import { selectById } from '@/queries';

/**
 * Reactive existence check. Using a single-column subscription instead of the
 * full-row subscription means the detail shell only re-renders when the row
 * appears/disappears — not on every field edit.
 */
export function useProductExists(productId: number): boolean {
  const rows = useQuery(
    selectById('products', 'id', productId),
    ([id]) => id as number,
  );
  return rows.length > 0;
}
