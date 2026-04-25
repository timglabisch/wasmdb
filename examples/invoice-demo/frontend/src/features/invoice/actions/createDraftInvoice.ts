import { useCallback } from 'react';
import { useNavigate } from '@tanstack/react-router';
import { executeOnStream, createStream, flushStream, nextId, peekQuery } from '../../../wasm.ts';
import { createInvoice } from '../../../commands/invoice/createInvoice.ts';
import { logActivity } from '../../../commands/activity/logActivity.ts';
import { isoDate } from './isoDate.ts';

const DOC_PREFIX = 'INV';

interface CustomerDefaults {
  payment_terms_days: number;
  billing_street: string; billing_zip: string; billing_city: string; billing_country: string;
  shipping_street: string; shipping_zip: string; shipping_city: string; shipping_country: string;
}

function peekCustomerDefaults(customerId: string): CustomerDefaults | null {
  if (!customerId) return null;
  const rows = peekQuery(
    `SELECT customers.payment_terms_days, ` +
    `customers.billing_street, customers.billing_zip, customers.billing_city, customers.billing_country, ` +
    `customers.shipping_street, customers.shipping_zip, customers.shipping_city, customers.shipping_country ` +
    `FROM customers WHERE customers.id = UUID '${customerId}'`,
  );
  if (rows.length === 0) return null;
  const r = rows[0];
  return {
    payment_terms_days: r[0] as number,
    billing_street: r[1] as string, billing_zip: r[2] as string,
    billing_city: r[3] as string, billing_country: r[4] as string,
    shipping_street: r[5] as string, shipping_zip: r[6] as string,
    shipping_city: r[7] as string, shipping_country: r[8] as string,
  };
}

/**
 * Creates a draft invoice and navigates into it. The customer is optional —
 * pass 0/undefined to create a blank draft that the user will fill in inside
 * the editor. When a customer is given, their address + payment-terms defaults
 * are pre-filled so the common path is still one click.
 */
export function useCreateDraftInvoice() {
  const navigate = useNavigate();
  return useCallback(async (customerId: string = '') => {
    const id = nextId();
    const defaults = customerId ? peekCustomerDefaults(customerId) : null;
    const dueDays = defaults?.payment_terms_days ?? 14;
    const number = `${DOC_PREFIX}-2026-${id.slice(0, 8)}`;
    const stream = createStream(8);
    executeOnStream(stream, createInvoice({
      id, customer_id: customerId, number,
      status: 'draft',
      date_issued: isoDate(0), date_due: isoDate(dueDays),
      doc_type: 'invoice',
      billing_street: defaults?.billing_street ?? '',
      billing_zip: defaults?.billing_zip ?? '',
      billing_city: defaults?.billing_city ?? '',
      billing_country: defaults?.billing_country ?? 'DE',
      shipping_street: defaults?.shipping_street ?? '',
      shipping_zip: defaults?.shipping_zip ?? '',
      shipping_city: defaults?.shipping_city ?? '',
      shipping_country: defaults?.shipping_country ?? 'DE',
    }));
    executeOnStream(stream, logActivity({
      entityType: 'invoice', entityId: id,
      action: 'create', detail: `Rechnung "${number}" angelegt (Entwurf)`,
    }));
    await flushStream(stream);
    navigate({ to: '/invoices/$invoiceId', params: { invoiceId: id } });
  }, [navigate]);
}
