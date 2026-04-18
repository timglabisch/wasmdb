import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteSepaMandate' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: 0 };

export function deleteSepaMandate(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeleteSepaMandate', ...DEFAULTS, ...args };
}
