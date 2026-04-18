/** Extract up to two initials from a name for avatar fallbacks. */
export function initialsOf(name: string): string {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return '?';
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0]! + parts[parts.length - 1]![0]!).toUpperCase();
}

/** Today's date in ISO YYYY-MM-DD. */
export function todayISO(): string {
  return new Date().toISOString().slice(0, 10);
}

/**
 * Masks an IBAN: keeps the country+first 4 and last 4, stars the middle,
 * grouping in blocks of four.
 */
export function maskIban(iban: string): string {
  const clean = iban.replace(/\s+/g, '').toUpperCase();
  if (clean.length <= 8) return clean;
  const head = clean.slice(0, 4);
  const tail = clean.slice(-4);
  const midLen = clean.length - 8;
  const masked = '*'.repeat(midLen);
  const full = head + masked + tail;
  return full.match(/.{1,4}/g)?.join(' ') ?? full;
}
