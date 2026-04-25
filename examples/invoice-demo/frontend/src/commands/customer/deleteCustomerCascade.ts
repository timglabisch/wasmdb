import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteCustomerCascade' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: '' };

/**
 * Full tree delete: customer + contacts + invoices + positions + payments +
 * sepa_mandates + recurring templates + recurring positions, all atomic.
 */
export function deleteCustomerCascade(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeleteCustomerCascade', ...DEFAULTS, ...args };
}
