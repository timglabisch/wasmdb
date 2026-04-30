import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'ConvertOfferToInvoice' }>;

/** Build a ConvertOfferToInvoice intent command. Sets doc_type='invoice' and
 * status='draft', and emits an offer_converted activity-log row — all inside
 * the command itself (see `commands/invoice/convert_offer_to_invoice.rs`). */
export function convertOfferToInvoice(invoiceId: string): InvoiceCommand {
  const cmd: Variant = {
    type: 'ConvertOfferToInvoice',
    id: invoiceId,
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
  };
  return cmd;
}
