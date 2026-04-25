import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'UpdateSepaMandate' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', mandate_ref: '',
  iban: '', bic: '', holder_name: '', signed_at: '', status: 'active',
};

export function updateSepaMandate(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'UpdateSepaMandate', ...DEFAULTS, ...args };
}
