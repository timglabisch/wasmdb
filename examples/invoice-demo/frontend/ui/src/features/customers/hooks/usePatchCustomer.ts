import { useCallback } from 'react';
import { execute } from '@/commands';
import { updateCustomer } from '@/generated/InvoiceCommandFactories';
import { peekCustomer } from '../reads/peekCustomer';
import type { CustomerWithoutPk } from '@/generated/tables/Customer';

/**
 * Stable `patch(partial)` callback that composes the full UpdateCustomer
 * payload at write time via peekCustomer. The caller does not subscribe to
 * customer columns — re-renders only happen when customerId changes.
 */
export function usePatchCustomer(customerId: string) {
  return useCallback((partial: Partial<Omit<CustomerWithoutPk, 'created_at'>>) => {
    const row = peekCustomer(customerId);
    if (!row) return;
    const { created_at, ...patchable } = row;
    execute(updateCustomer({ ...patchable, id: customerId, ...partial }));
  }, [customerId]);
}
