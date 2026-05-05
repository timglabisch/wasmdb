import { useCallback } from 'react';

interface Props {
  direction: 'horizontal' | 'vertical';
  onDrag: (delta: number) => void;
  testid?: string;
}

export function Splitter({ direction, onDrag, testid }: Props) {
  const onMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    let last = direction === 'horizontal' ? e.clientX : e.clientY;
    const onMove = (ev: MouseEvent) => {
      const cur = direction === 'horizontal' ? ev.clientX : ev.clientY;
      const delta = cur - last;
      if (delta !== 0) {
        onDrag(delta);
        last = cur;
      }
    };
    const onUp = () => {
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
    document.body.style.cursor = direction === 'horizontal' ? 'col-resize' : 'row-resize';
    document.body.style.userSelect = 'none';
  }, [direction, onDrag]);

  return (
    <div
      className={`splitter splitter-${direction}`}
      onMouseDown={onMouseDown}
      role="separator"
      aria-orientation={direction === 'horizontal' ? 'vertical' : 'horizontal'}
      data-testid={testid}
    />
  );
}
