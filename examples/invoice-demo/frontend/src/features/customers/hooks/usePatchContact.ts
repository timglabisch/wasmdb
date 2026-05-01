import { useCallback } from 'react';
import { execute } from '@/wasm';
import { updateContact } from '@/generated/InvoiceCommandFactories';
import { peekContact } from '../reads/peekCustomer';
import type { ContactRow } from '../types';

/** Stable patch for a contact row. */
export function usePatchContact(contactId: string) {
  return useCallback((partial: Partial<ContactRow>) => {
    const row = peekContact(contactId);
    if (!row) return;
    execute(updateContact({ ...row, id: contactId, ...partial }));
  }, [contactId]);
}
