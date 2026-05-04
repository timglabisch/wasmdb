/**
 * Internationalized formatters. All money is stored as cent (i64).
 */
const eur = new Intl.NumberFormat('de-DE', {
  style: 'currency',
  currency: 'EUR',
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const dateFmt = new Intl.DateTimeFormat('de-DE', {
  year: 'numeric', month: '2-digit', day: '2-digit',
});

export const formatEuro = (cents: number): string => eur.format(cents / 100);

export const formatDateISO = (iso: string): string => {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return dateFmt.format(d);
};

export const relativeDaysFromToday = (iso: string): number => {
  if (!iso) return 0;
  const then = Date.parse(iso);
  if (Number.isNaN(then)) return 0;
  const now = Date.parse(new Date().toISOString().slice(0, 10));
  return Math.floor((then - now) / (1000 * 60 * 60 * 24));
};
