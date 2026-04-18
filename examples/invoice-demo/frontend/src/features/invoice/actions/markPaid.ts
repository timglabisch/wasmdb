import { executeOnStream, createStream, flushStream } from '../../../wasm.ts';
import { updateInvoiceHeader } from '../../../commands/invoice/updateInvoiceHeader.ts';
import { logActivity } from '../../../commands/activity/logActivity.ts';
import { peekInvoice } from '../reads/peekInvoice.ts';

/** Set an invoice's status to `paid` + log the status change, atomic. */
export async function markPaid(invoiceId: number): Promise<void> {
  const inv = peekInvoice(invoiceId);
  if (!inv) return;
  const stream = createStream(8);
  executeOnStream(stream, updateInvoiceHeader({ ...inv, id: invoiceId, status: 'paid' }));
  executeOnStream(stream, logActivity({
    entityType: 'invoice', entityId: invoiceId,
    action: 'status_paid', detail: `"${inv.number}" als bezahlt markiert`,
  }));
  await flushStream(stream);
}
