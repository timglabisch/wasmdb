/**
 * Money/tax helpers.
 *
 * Units across the demo:
 *   - cents (i64): all prices
 *   - 1/1000 (i64): quantities (so 1.500 → 1500)
 *   - basis points (i64): tax rate + discount (1900 = 19%)
 */

export const formatCents = (cents: number): string =>
  (cents / 100).toFixed(2) + '€';

export const formatBp = (bp: number): string =>
  (bp / 100).toFixed(1) + '%';

export const formatQty = (q: number): string => (q / 1000).toFixed(2);

export const isoToday = (offsetDays = 0): string => {
  const d = new Date();
  d.setDate(d.getDate() + offsetDays);
  return d.toISOString().slice(0, 10);
};

export const isOverdue = (dateDue: string, status: string, today = isoToday()): boolean =>
  status !== 'paid' && status !== 'cancelled' && dateDue.length > 0 && dateDue < today;

export type PaymentStatus = 'unpaid' | 'partial' | 'paid' | 'overpaid';

export const paymentStatus = (grossCents: number, paidCents: number): PaymentStatus => {
  if (paidCents <= 0) return 'unpaid';
  if (paidCents >= grossCents) {
    return paidCents > grossCents ? 'overpaid' : 'paid';
  }
  return 'partial';
};

export const statusLabel = (s: PaymentStatus): string => ({
  unpaid: 'offen',
  partial: 'teilweise',
  paid: 'bezahlt',
  overpaid: 'überzahlt',
})[s];

export const statusClass = (s: PaymentStatus): string => `pay-status pay-${s}`;
