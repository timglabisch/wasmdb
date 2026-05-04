import { createStream, flushStream, nextId } from '@wasmdb/client';
import { executeOnStream } from '@/commands';
import { storno } from 'invoice-demo-generated/InvoiceCommandFactories';
import { peekInvoice } from '../reads/peekInvoice.ts';
import { peekPositions } from '../reads/peekPositions.ts';
import { isoDate } from './isoDate.ts';

/**
 * Storno: mark the invoice cancelled AND emit a mirror credit note in the same
 * atomic stream. Keeps the audit trail clean for accounting.
 *
 * The optimistic apply (status update + credit note + positions + activity row)
 * happens synchronously inside the `Storno` intent command. Callers no longer
 * compose separate writes.
 */
export async function stornoInvoice(invoiceId: string): Promise<string | null> {
  const inv = peekInvoice(invoiceId);
  if (!inv) return null;
  if (!confirm(`"${inv.number}" stornieren (status=cancelled) und Gutschrift erzeugen?`)) return null;
  const positions = peekPositions(invoiceId);
  const creditNoteId = nextId();
  const stream = createStream(4);
  executeOnStream(stream, storno({
    id: invoiceId,
    credit_note_id: creditNoteId,
    customer_id: inv.customer_id,
    credit_note_number: `CN-${inv.number}`,
    date_issued: isoDate(0),
    date_due: isoDate(14),
    notes: inv.notes,
    service_date: inv.service_date,
    cash_allowance_pct: inv.cash_allowance_pct,
    cash_allowance_days: inv.cash_allowance_days,
    discount_pct: inv.discount_pct,
    payment_method: inv.payment_method,
    sepa_mandate_id: inv.sepa_mandate_id,
    currency: inv.currency,
    language: inv.language,
    project_ref: inv.project_ref,
    external_id: inv.external_id,
    billing_street: inv.billing_street,
    billing_zip: inv.billing_zip,
    billing_city: inv.billing_city,
    billing_country: inv.billing_country,
    shipping_street: inv.shipping_street,
    shipping_zip: inv.shipping_zip,
    shipping_city: inv.shipping_city,
    shipping_country: inv.shipping_country,
    positions: positions.map((p) => ({
      id: nextId(),
      position_nr: p.position_nr,
      description: p.description,
      quantity: -p.quantity,
      unit_price: p.unit_price,
      tax_rate: p.tax_rate,
      product_id: p.product_id,
      item_number: p.item_number,
      unit: p.unit,
      discount_pct: p.discount_pct,
      cost_price: p.cost_price,
      position_type: p.position_type,
    })),
  }));
  await flushStream(stream);
  return creditNoteId;
}
