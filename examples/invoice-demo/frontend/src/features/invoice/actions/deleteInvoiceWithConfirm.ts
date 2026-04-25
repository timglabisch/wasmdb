import { executeOnStream, createStream, flushStream } from '../../../wasm.ts';
import { deleteInvoice } from '../../../commands/invoice/deleteInvoice.ts';
import { logActivity } from '../../../commands/activity/logActivity.ts';

/**
 * Delete the invoice (cascades through positions/payments on the Rust side)
 * after a confirm dialog, plus an audit log entry. Returns true if deleted.
 */
export async function deleteInvoiceWithConfirm(invoiceId: string, number: string): Promise<boolean> {
  if (!confirm(`Beleg "${number}" inkl. aller Positionen & Zahlungen löschen?`)) return false;
  const stream = createStream(8);
  executeOnStream(stream, deleteInvoice({ id: invoiceId }));
  executeOnStream(stream, logActivity({
    entityType: 'invoice', entityId: invoiceId,
    action: 'delete', detail: `Beleg "${number}" gelöscht`,
  }));
  await flushStream(stream);
  return true;
}
