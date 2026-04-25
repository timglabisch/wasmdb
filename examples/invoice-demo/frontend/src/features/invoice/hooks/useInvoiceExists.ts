import { useEffect, useState } from 'react';
import { peekQueryAsync, useQuery } from '@/wasm';

export type ExistenceState = 'loading' | 'found' | 'notfound';

/**
 * Reactive existence check that also primes the local invoices table.
 *
 * Direct deep-link navigation lands here before anything else has triggered
 * the `invoices.all()` fetcher. Without priming, the local table is empty
 * and `WHERE id = X` returns 0 rows — falsely claiming the invoice does not
 * exist. We do a one-shot async read against the fetcher to guarantee the
 * table is populated, then use a normal reactive subscription on the table
 * for live updates (deletions, etc).
 */
export function useInvoiceExists(invoiceId: string): ExistenceState {
  const [primed, setPrimed] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setPrimed(false);
    peekQueryAsync(`SELECT invoices.id FROM invoices.all()`)
      .finally(() => {
        if (cancelled) return;
        setPrimed(true);
      });
    return () => { cancelled = true; };
  }, [invoiceId]);

  const rows = useQuery(
    `SELECT invoices.id FROM invoices WHERE invoices.id = UUID '${invoiceId}'`,
    ([id]) => id as string,
  );

  if (!primed) return 'loading';
  return rows.length > 0 ? 'found' : 'notfound';
}
