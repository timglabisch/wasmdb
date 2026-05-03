import { useCallback } from 'react';
import { execute } from '@/wasm';
import { updateContact } from '@/generated/InvoiceCommandFactories';
import { peekContact } from '../reads/peekCustomer';
import type { ContactWithoutPk } from '@/generated/tables/Contact';

/** Stable patch for a contact row. */
export function usePatchContact(contactId: string) {
  return useCallback((partial: Partial<Omit<ContactWithoutPk, 'customer_id'>>) => {
    const row = peekContact(contactId);
    if (!row) return;
    const { customer_id, ...patchable } = row;
    execute(updateContact({ ...patchable, id: contactId, ...partial }));
  }, [contactId]);
}
