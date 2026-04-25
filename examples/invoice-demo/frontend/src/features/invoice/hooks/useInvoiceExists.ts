import { useQuery } from '../../../wasm.ts';

/**
 * Reactive existence check. Using a single-column subscription instead of the
 * full-row subscription means the detail shell only re-renders when the row
 * appears/disappears — not on every field edit.
 */
export function useInvoiceExists(invoiceId: string): boolean {
  const rows = useQuery(
    `SELECT invoices.id FROM invoices WHERE invoices.id = UUID '${invoiceId}'`,
    ([id]) => id as string,
  );
  return rows.length > 0;
}
