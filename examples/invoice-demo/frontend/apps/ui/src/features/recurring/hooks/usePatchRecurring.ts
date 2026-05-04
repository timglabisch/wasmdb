import { useCallback } from 'react';
import { execute } from '@/commands';
import { updateRecurring } from 'invoice-demo-generated/InvoiceCommandFactories';
import { peekRecurring } from '../reads/peekRecurring';
import type { RecurringInvoiceWithoutPk } from 'invoice-demo-generated/tables/RecurringInvoice';

/**
 * Stable `patch(partial)` callback that composes the full UpdateRecurring
 * payload at write time via peekRecurring. The caller does not subscribe to
 * recurring columns — re-renders only happen when recurringId changes.
 *
 * Note: UpdateRecurring does not accept customer_id — customer changes are
 * therefore ignored at the write-time level; callers should not pass one.
 */
export function usePatchRecurring(recurringId: string) {
  return useCallback((partial: Partial<Omit<RecurringInvoiceWithoutPk, 'customer_id' | 'last_run'>>) => {
    const row = peekRecurring(recurringId);
    if (!row) return;
    const merged = { ...row, ...partial };
    execute(updateRecurring({
      id: recurringId,
      template_name: merged.template_name,
      interval_unit: merged.interval_unit,
      interval_value: merged.interval_value,
      next_run: merged.next_run,
      enabled: merged.enabled,
      status_template: merged.status_template,
      notes_template: merged.notes_template,
    }));
  }, [recurringId]);
}
