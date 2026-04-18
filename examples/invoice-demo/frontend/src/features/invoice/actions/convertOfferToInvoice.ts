import { execute } from '../../../wasm.ts';
import { updateInvoiceHeader } from '../../../commands/invoice/updateInvoiceHeader.ts';
import { peekInvoice } from '../reads/peekInvoice.ts';

/** Convert an offer into a fresh draft invoice — fires a single UpdateInvoiceHeader. */
export function convertOfferToInvoice(invoiceId: number): void {
  const inv = peekInvoice(invoiceId);
  if (!inv) return;
  execute(updateInvoiceHeader({ ...inv, id: invoiceId, doc_type: 'invoice', status: 'draft' }));
}
