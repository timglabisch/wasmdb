import { useQuery } from '../../../wasm.ts';
import { selectById } from '../../../queries.ts';

/**
 * Reactive existence check. Using a single-column subscription instead of the
 * full-row subscription means the detail shell only re-renders when the row
 * appears/disappears — not on every field edit.
 */
export function useInvoiceExists(invoiceId: number): boolean {
  const rows = useQuery(
    selectById('invoices', 'id', invoiceId),
    ([id]) => id as number,
  );
  return rows.length > 0;
}
