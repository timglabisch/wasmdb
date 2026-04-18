import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteInvoice' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: 0 };

/** Cascading delete — drops the invoice plus its positions and payments. */
export function deleteInvoice(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeleteInvoice', ...DEFAULTS, ...args };
}
