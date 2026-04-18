import { useCallback } from 'react';
import { execute } from '../../../wasm.ts';
import { updateInvoiceHeader } from '../../../commands/invoice/updateInvoiceHeader.ts';
import { peekInvoice } from '../reads/peekInvoice.ts';
import type { InvoiceRow } from '../types.ts';

/**
 * Build a `patch(partial)` callback that is stable across renders and composes
 * the required full-row payload at write time using peekInvoice. Caller does
 * not subscribe to invoice columns — re-renders only happen when invoiceId
 * changes.
 */
export function usePatchInvoice(invoiceId: number) {
  return useCallback((partial: Partial<InvoiceRow>) => {
    const inv = peekInvoice(invoiceId);
    if (!inv) return;
    execute(updateInvoiceHeader({ ...inv, id: invoiceId, ...partial }));
  }, [invoiceId]);
}
