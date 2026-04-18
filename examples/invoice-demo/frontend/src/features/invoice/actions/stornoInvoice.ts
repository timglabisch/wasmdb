import { executeOnStream, createStream, flushStream, nextId } from '../../../wasm.ts';
import { createInvoice } from '../../../commands/invoice/createInvoice.ts';
import { updateInvoiceHeader } from '../../../commands/invoice/updateInvoiceHeader.ts';
import { addPosition } from '../../../commands/position/addPosition.ts';
import { logActivity } from '../../../commands/activity/logActivity.ts';
import { peekInvoice } from '../reads/peekInvoice.ts';
import { peekPositions } from '../reads/peekPositions.ts';
import { isoDate } from './isoDate.ts';

/**
 * Storno: mark the invoice cancelled AND emit a mirror credit note in the same
 * atomic stream. Keeps the audit trail clean for accounting.
 */
export async function stornoInvoice(invoiceId: number): Promise<number | null> {
  const inv = peekInvoice(invoiceId);
  if (!inv) return null;
  if (!confirm(`"${inv.number}" stornieren (status=cancelled) und Gutschrift erzeugen?`)) return null;
  const positions = peekPositions(invoiceId);
  const stream = createStream(64);
  executeOnStream(stream, updateInvoiceHeader({ ...inv, id: invoiceId, status: 'cancelled' }));
  const newId = nextId();
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
    entityType: 'invoice', entityId: invoiceId,
    action: 'storno',
    detail: `"${inv.number}" storniert, Gutschrift #${newId} erstellt`,
  }));
  await flushStream(stream);
  return newId;
}
