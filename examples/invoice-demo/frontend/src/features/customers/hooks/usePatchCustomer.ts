import { useCallback } from 'react';
import { execute } from '@/wasm';
import { updateCustomer } from '@/commands/customer/updateCustomer';
import { peekCustomer } from '../reads/peekCustomer';
import type { CustomerRow } from '../types';

/**
 * Stable `patch(partial)` callback that composes the full UpdateCustomer
 * payload at write time via peekCustomer. The caller does not subscribe to
 * customer columns — re-renders only happen when customerId changes.
 */
export function usePatchCustomer(customerId: string) {
  return useCallback((partial: Partial<CustomerRow>) => {
    const row = peekCustomer(customerId);
    if (!row) return;
    execute(updateCustomer({ ...row, id: customerId, ...partial }));
  }, [customerId]);
}
