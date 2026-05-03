import { createStream, flushStream } from '@wasmdb/client';
import { executeOnStream } from '@/commands';
import { deleteInvoice } from '../../../generated/InvoiceCommandFactories.ts';

/**
 * Delete the invoice (cascades through positions/payments on the Rust side)
 * after a confirm dialog, plus an audit log entry. Returns true if deleted.
 * The activity row is written by the DeleteInvoice command itself.
 */
export async function deleteInvoiceWithConfirm(invoiceId: string, number: string): Promise<boolean> {
  if (!confirm(`Beleg "${number}" inkl. aller Positionen & Zahlungen löschen?`)) return false;
  const stream = createStream(8);
  executeOnStream(stream, deleteInvoice({ id: invoiceId, number }));
  await flushStream(stream);
  return true;
}
