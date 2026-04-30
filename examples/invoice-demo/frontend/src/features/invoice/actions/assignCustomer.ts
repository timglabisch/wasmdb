import { executeOnStream, createStream, flushStream, peekQuery } from '../../../wasm.ts';
import { assignCustomer as assignCustomerCmd } from '../../../commands/invoice/assignCustomer.ts';
import { peekInvoice } from '../reads/peekInvoice.ts';
import { isoDate } from './isoDate.ts';

interface CustomerDefaults {
  name: string;
  payment_terms_days: number;
  billing_street: string; billing_zip: string; billing_city: string; billing_country: string;
  shipping_street: string; shipping_zip: string; shipping_city: string; shipping_country: string;
}

function peekCustomer(customerId: string | null): CustomerDefaults | null {
  if (!customerId) return null;
  const rows = peekQuery(
    `SELECT customers.name, customers.payment_terms_days, ` +
    `customers.billing_street, customers.billing_zip, customers.billing_city, customers.billing_country, ` +
    `customers.shipping_street, customers.shipping_zip, customers.shipping_city, customers.shipping_country ` +
    `FROM customers WHERE customers.id = UUID '${customerId}'`,
  );
  if (rows.length === 0) return null;
  const r = rows[0];
  return {
    name: r[0] as string,
    payment_terms_days: r[1] as number,
    billing_street: r[2] as string, billing_zip: r[3] as string,
    billing_city: r[4] as string, billing_country: r[5] as string,
    shipping_street: r[6] as string, shipping_zip: r[7] as string,
    shipping_city: r[8] as string, shipping_country: r[9] as string,
  };
}

const addrIsEmpty = (inv: {
  billing_street: string; billing_zip: string; billing_city: string;
  shipping_street: string; shipping_zip: string; shipping_city: string;
}) =>
  !inv.billing_street && !inv.billing_zip && !inv.billing_city &&
  !inv.shipping_street && !inv.shipping_zip && !inv.shipping_city;

/**
 * Assigns a customer to an invoice. When the invoice's address fields are still
 * empty, pulls the customer's billing/shipping defaults into the command payload —
 * and derives a sensible date_due from their payment-terms. Existing addresses are
 * kept as-is so we never clobber user edits.
 *
 * The `AssignCustomer` intent command writes customer_id, address fields, date_due,
 * and the activity-log row atomically — callers no longer compose separate writes.
 */
export async function assignCustomer(invoiceId: string, customerId: string | null): Promise<void> {
  const inv = peekInvoice(invoiceId);
  if (!inv) return;
  const cust = peekCustomer(customerId);
  const copyAddr = cust && addrIsEmpty(inv);
  const stream = createStream(2);
  executeOnStream(stream, assignCustomerCmd({
    invoiceId,
    customerId,
    customerName: cust ? cust.name : '',
    billingStreet:   copyAddr ? cust.billing_street   : inv.billing_street,
    billingZip:      copyAddr ? cust.billing_zip      : inv.billing_zip,
    billingCity:     copyAddr ? cust.billing_city     : inv.billing_city,
    billingCountry:  copyAddr ? cust.billing_country  : inv.billing_country,
    shippingStreet:  copyAddr ? cust.shipping_street  : inv.shipping_street,
    shippingZip:     copyAddr ? cust.shipping_zip     : inv.shipping_zip,
    shippingCity:    copyAddr ? cust.shipping_city    : inv.shipping_city,
    shippingCountry: copyAddr ? cust.shipping_country : inv.shipping_country,
    dateDue: copyAddr && cust.payment_terms_days > 0
      ? isoDate(cust.payment_terms_days)
      : inv.date_due,
  }));
  await flushStream(stream);
}
