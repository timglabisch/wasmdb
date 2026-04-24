import { useQuery } from '../../../wasm.ts';

/**
 * Reactive existence check. Using a single-column subscription instead of the
 * full-row subscription means the detail shell only re-renders when the row
 * appears/disappears — not on every field edit.
 */
export function useInvoiceExists(invoiceId: number): boolean {
  const rows = useQuery(
    `SELECT invoices.id FROM invoices WHERE invoices.id = ${invoiceId}`,
    ([id]) => id as number,
  );
  return rows.length > 0;
}
