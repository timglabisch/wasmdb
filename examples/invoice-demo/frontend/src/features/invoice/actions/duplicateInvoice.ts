import { executeOnStream, createStream, flushStream, nextId } from '../../../wasm.ts';
import { createInvoice } from '../../../commands/invoice/createInvoice.ts';
import { addPosition } from '../../../commands/position/addPosition.ts';
import { logActivity } from '../../../commands/activity/logActivity.ts';
import { peekInvoice } from '../reads/peekInvoice.ts';
import { peekPositions } from '../reads/peekPositions.ts';
import { isoDate } from './isoDate.ts';

/**
 * Duplicate an invoice: copy header + all positions into a new draft.
 * Returns the new id, or null if the source doesn't exist.
 */
export async function duplicateInvoice(invoiceId: string): Promise<string | null> {
  const inv = peekInvoice(invoiceId);
  if (!inv) return null;
  const positions = peekPositions(invoiceId);
  const newId = nextId();
  const newNumber = `${inv.number}-KOPIE`;
  const stream = createStream(64);
  executeOnStream(stream, createInvoice({
    ...inv, id: newId, customer_id: inv.customer_id,
    number: newNumber, status: 'draft',
    date_issued: isoDate(0), date_due: isoDate(14),
    parent_id: null,
  }));
  for (const p of positions) {
    executeOnStream(stream, addPosition({
      id: nextId(), invoice_id: newId,
      position_nr: p.position_nr,
      description: p.description, quantity: p.quantity, unit_price: p.unit_price,
      tax_rate: p.tax_rate, product_id: p.product_id, item_number: p.item_number,
      unit: p.unit, discount_pct: p.discount_pct, cost_price: p.cost_price,
      position_type: p.position_type,
    }));
  }
  executeOnStream(stream, logActivity({
    entityType: 'invoice', entityId: newId,
    action: 'duplicate_from',
    detail: `Kopie von "${inv.number}" als ${newNumber} angelegt`,
  }));
  await flushStream(stream);
  return newId;
}
