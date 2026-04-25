import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'CreateSepaMandate' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', customer_id: '', mandate_ref: '',
  iban: '', bic: '', holder_name: '', signed_at: '',
};

export function createSepaMandate(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'CreateSepaMandate', ...DEFAULTS, ...args };
}
