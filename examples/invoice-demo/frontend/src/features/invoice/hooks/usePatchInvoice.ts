import { useCallback } from 'react';
import { execute } from '../../../wasm.ts';
import { updateInvoiceHeader } from '../../../generated/InvoiceCommandFactories.ts';
import { peekInvoice } from '../reads/peekInvoice.ts';
import type { InvoiceRow } from '../types.ts';

/**
 * Build a `patch(partial)` callback that is stable across renders and composes
 * the required full-row payload at write time using peekInvoice. Caller does
 * not subscribe to invoice columns — re-renders only happen when invoiceId
 * changes.
 */
export function usePatchInvoice(invoiceId: string) {
  return useCallback((partial: Partial<InvoiceRow>) => {
    const inv = peekInvoice(invoiceId);
    if (!inv) return;
    const merged = { ...inv, ...partial };
    execute(updateInvoiceHeader({
      id: invoiceId,
      number: merged.number,
      status: merged.status,
      date_issued: merged.date_issued,
      date_due: merged.date_due,
      notes: merged.notes,
      doc_type: merged.doc_type,
      parent_id: merged.parent_id,
      service_date: merged.service_date,
      cash_allowance_pct: merged.cash_allowance_pct,
      cash_allowance_days: merged.cash_allowance_days,
      discount_pct: merged.discount_pct,
      payment_method: merged.payment_method,
      sepa_mandate_id: merged.sepa_mandate_id,
      currency: merged.currency,
      language: merged.language,
      project_ref: merged.project_ref,
      external_id: merged.external_id,
      billing_street: merged.billing_street,
      billing_zip: merged.billing_zip,
      billing_city: merged.billing_city,
      billing_country: merged.billing_country,
      shipping_street: merged.shipping_street,
      shipping_zip: merged.shipping_zip,
      shipping_city: merged.shipping_city,
      shipping_country: merged.shipping_country,
    }));
  }, [invoiceId]);
}
