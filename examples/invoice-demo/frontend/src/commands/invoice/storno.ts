import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import type { ActionPosition } from '../../features/invoice/reads/peekPositions.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'Storno' }>;

export interface StornoInput {
  invoiceId: string;
  invoice: {
    number: string;
    customer_id: string | null;
    notes: string;
    service_date: string;
    cash_allowance_pct: number;
    cash_allowance_days: number;
    discount_pct: number;
    payment_method: string;
    sepa_mandate_id: string | null;
    currency: string;
    language: string;
    project_ref: string;
    external_id: string;
    billing_street: string;
    billing_zip: string;
    billing_city: string;
    billing_country: string;
    shipping_street: string;
    shipping_zip: string;
    shipping_city: string;
    shipping_country: string;
  };
  creditNoteId: string;
  creditNoteNumber: string;
  dateIssued: string;
  dateDue: string;
  positions: ActionPosition[];
}

/** Build a Storno intent command. The credit-note invoice, its positions, and
 * the activity-log row are all emitted by the command itself
 * (see `commands/invoice/storno.rs`) — caller does not compose separate writes. */
export function storno(input: StornoInput): InvoiceCommand {
  const { invoiceId, invoice, creditNoteId, creditNoteNumber, dateIssued, dateDue, positions } = input;
  const cmd: Variant = {
    type: 'Storno',
    id: invoiceId,
    credit_note_id: creditNoteId,
    customer_id: invoice.customer_id,
    credit_note_number: creditNoteNumber,
    date_issued: dateIssued,
    date_due: dateDue,
    notes: invoice.notes,
    service_date: invoice.service_date,
    cash_allowance_pct: invoice.cash_allowance_pct,
    cash_allowance_days: invoice.cash_allowance_days,
    discount_pct: invoice.discount_pct,
    payment_method: invoice.payment_method,
    sepa_mandate_id: invoice.sepa_mandate_id,
    currency: invoice.currency,
    language: invoice.language,
    project_ref: invoice.project_ref,
    external_id: invoice.external_id,
    billing_street: invoice.billing_street,
    billing_zip: invoice.billing_zip,
    billing_city: invoice.billing_city,
    billing_country: invoice.billing_country,
    shipping_street: invoice.shipping_street,
    shipping_zip: invoice.shipping_zip,
    shipping_city: invoice.shipping_city,
    shipping_country: invoice.shipping_country,
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
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
  };
  return cmd;
}
