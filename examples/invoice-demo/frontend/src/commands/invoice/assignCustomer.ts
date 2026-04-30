import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'AssignCustomer' }>;

export interface AssignCustomerInput {
  invoiceId: string;
  customerId: string | null;
  customerName: string;
  billingStreet: string;
  billingZip: string;
  billingCity: string;
  billingCountry: string;
  shippingStreet: string;
  shippingZip: string;
  shippingCity: string;
  shippingCountry: string;
  dateDue: string;
}

/** Build an AssignCustomer intent command. Updates customer_id, optionally
 * copies address fields, and emits a customer_assigned activity-log row —
 * all inside the command itself (see `commands/invoice/assign_customer.rs`). */
export function assignCustomer(input: AssignCustomerInput): InvoiceCommand {
  const cmd: Variant = {
    type: 'AssignCustomer',
    id: input.invoiceId,
    customer_id: input.customerId,
    customer_name: input.customerName,
    billing_street: input.billingStreet,
    billing_zip: input.billingZip,
    billing_city: input.billingCity,
    billing_country: input.billingCountry,
    shipping_street: input.shippingStreet,
    shipping_zip: input.shippingZip,
    shipping_city: input.shippingCity,
    shipping_country: input.shippingCountry,
    date_due: input.dateDue,
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
  };
  return cmd;
}
