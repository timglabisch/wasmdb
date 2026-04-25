import { useCallback } from 'react';
import { execute } from '@/wasm';
import { updateSepaMandate } from '@/commands/sepaMandate/updateSepaMandate';
import { peekSepaMandate } from '../reads/peekCustomer';
import type { SepaMandateRow } from '../types';

/** Stable patch for a sepa_mandates row. */
export function usePatchSepaMandate(mandateId: string) {
  return useCallback((partial: Partial<SepaMandateRow>) => {
    const row = peekSepaMandate(mandateId);
    if (!row) return;
    execute(updateSepaMandate({ ...row, id: mandateId, ...partial }));
  }, [mandateId]);
}
