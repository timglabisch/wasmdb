import { useEffect, useRef } from 'react';

/**
 * Visual feedback hook. Attach the returned ref to an element; after every
 * render past the first, the element briefly pulses (CSS animation
 * `render-flash`). Pure decoration — does not affect render counts or test
 * assertions.
 *
 * Skipping the first render keeps the page from flashing on initial mount,
 * which would otherwise drown the signal we actually care about
 * (re-renders triggered by reactivity).
 */
export function useRenderFlash<T extends HTMLElement>() {
  const ref = useRef<T>(null);
  const isFirst = useRef(true);
  useEffect(() => {
    if (isFirst.current) {
      isFirst.current = false;
      return;
    }
    const el = ref.current;
    if (!el) return;
    el.classList.remove('rendered-flash');
    void el.offsetHeight;
    el.classList.add('rendered-flash');
    const t = window.setTimeout(() => el.classList.remove('rendered-flash'), 700);
    return () => window.clearTimeout(t);
  });
  return ref;
}
