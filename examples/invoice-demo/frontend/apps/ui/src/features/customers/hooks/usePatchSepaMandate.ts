import { useCallback } from 'react';
import { execute } from '@/commands';
import { updateSepaMandate } from 'invoice-demo-generated/InvoiceCommandFactories';
import { peekSepaMandate } from '../reads/peekCustomer';
import type { SepaMandateWithoutPk } from 'invoice-demo-generated/tables/SepaMandate';

/** Stable patch for a sepa_mandates row. */
export function usePatchSepaMandate(mandateId: string) {
  return useCallback((partial: Partial<Omit<SepaMandateWithoutPk, 'customer_id'>>) => {
    const row = peekSepaMandate(mandateId);
    if (!row) return;
    const { customer_id, ...patchable } = row;
    execute(updateSepaMandate({ ...patchable, id: mandateId, ...partial }));
  }, [mandateId]);
}
