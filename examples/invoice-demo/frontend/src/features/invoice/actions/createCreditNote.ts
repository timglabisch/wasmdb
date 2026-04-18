import { executeOnStream, createStream, flushStream, nextId } from '../../../wasm.ts';
import { createInvoice } from '../../../commands/invoice/createInvoice.ts';
import { addPosition } from '../../../commands/position/addPosition.ts';
import { logActivity } from '../../../commands/activity/logActivity.ts';
import { peekInvoice } from '../reads/peekInvoice.ts';
import { peekPositions } from '../reads/peekPositions.ts';
import { isoDate } from './isoDate.ts';

/**
 * Create a credit note for an existing invoice: fresh draft with negated
 * quantities + parent_id link back to the source invoice.
 */
export async function createCreditNote(invoiceId: number): Promise<number | null> {
  const inv = peekInvoice(invoiceId);
  if (!inv) return null;
  if (!confirm(`Gutschrift zu "${inv.number}" erzeugen?`)) return null;
  const positions = peekPositions(invoiceId);
  const newId = nextId();
  const stream = createStream(64);
  executeOnStream(stream, createInvoice({
    ...inv, id: newId, customer_id: inv.customer_id,
    number: `CN-${inv.number}`, status: 'draft',
    date_issued: isoDate(0), date_due: isoDate(14),
    doc_type: 'credit_note', parent_id: invoiceId,
  }));
  for (const p of positions) {
    executeOnStream(stream, addPosition({
      id: nextId(), invoice_id: newId,
      position_nr: p.position_nr,
      description: p.description,
      quantity: -p.quantity, unit_price: p.unit_price,
      tax_rate: p.tax_rate, product_id: p.product_id, item_number: p.item_number,
      unit: p.unit, discount_pct: p.discount_pct, cost_price: p.cost_price,
      position_type: p.position_type,
    }));
  }
  executeOnStream(stream, logActivity({
    entityType: 'invoice', entityId: newId,
    action: 'credit_note_created',
    detail: `Gutschrift zu "${inv.number}" (#${invoiceId}) als #${newId} angelegt`,
  }));
  await flushStream(stream);
  return newId;
}
