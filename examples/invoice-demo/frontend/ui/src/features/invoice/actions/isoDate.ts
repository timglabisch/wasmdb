/** Local date helper so actions don't reach into shared/lib for formatting utilities. */
export function isoDate(days = 0): string {
  const d = new Date();
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
}
