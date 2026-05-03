import { useQuery } from '@wasmdb/client';
export interface GrossPosition {
  quantity: number;
  unit_price: number;
  tax_rate: number;
  discount_pct: number;
  position_type: string;
}

/**
 * Reactive planner rejects arithmetic inside SUM. So we subscribe to the raw
 * rows of an invoice and fold totals in JS.
 */
export function useInvoicePositions(invoiceId: string): GrossPosition[] {
  return useQuery(
    `SELECT positions.quantity, positions.unit_price, positions.tax_rate, ` +
    `positions.discount_pct, positions.position_type ` +
    `FROM positions WHERE REACTIVE(positions.invoice_id = UUID '${invoiceId}') ORDER BY positions.position_nr`,
    ([q, p, t, d, pt]) => ({
      quantity: q as number,
      unit_price: p as number,
      tax_rate: t as number,
      discount_pct: d as number,
      position_type: pt as string,
    }),
  );
}

export function computeNetCents(positions: GrossPosition[]): number {
  let net = 0;
  for (const p of positions) {
    if (p.position_type !== 'service' && p.position_type !== 'product') continue;
    const raw = (p.quantity * p.unit_price) / 1000;
    const afterDisc = Math.round(raw * (10000 - p.discount_pct) / 10000);
    net += afterDisc;
  }
  return net;
}

export function computeGrossCents(positions: GrossPosition[]): number {
  let gross = 0;
  for (const p of positions) {
    if (p.position_type !== 'service' && p.position_type !== 'product') continue;
    const raw = (p.quantity * p.unit_price) / 1000;
    const afterDisc = Math.round(raw * (10000 - p.discount_pct) / 10000);
    gross += Math.round(afterDisc * (10000 + p.tax_rate) / 10000);
  }
  return gross;
}

/** Count positions that contribute to totals. */
export function countCalculated(positions: GrossPosition[]): number {
  return positions.filter(p => p.position_type === 'service' || p.position_type === 'product').length;
}

/** One-shot: fetch + gross. Useful for badges/totals that only need the sum. */
export function useInvoiceGrossCents(invoiceId: string): number {
  return computeGrossCents(useInvoicePositions(invoiceId));
}

/** Global gross across all positions — for dashboard KPI. */
export function useGlobalGrossCents(): number {
  const rows = useQuery(
    'SELECT REACTIVE(positions.id), positions.quantity, positions.unit_price, positions.tax_rate, positions.discount_pct, positions.position_type FROM positions',
    ([_r, q, p, t, d, pt]) => ({
      quantity: q as number,
      unit_price: p as number,
      tax_rate: t as number,
      discount_pct: d as number,
      position_type: pt as string,
    }),
  );
  return computeGrossCents(rows);
}
