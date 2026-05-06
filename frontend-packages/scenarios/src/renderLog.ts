/** Reset the global render-log map. Used by the scenario layout's
 *  "Reset render counts" button and by e2e harnesses between scenarios. */
export function resetRenderLog(): void {
  if (typeof window !== 'undefined') {
    const w = window as unknown as { __renderLog?: Map<string, number> };
    w.__renderLog = new Map();
  }
}
