import { useCallback } from 'react';
import { execute } from '@/wasm';
import { updateProduct } from '@/commands/product/updateProduct';
import { peekProduct } from '../reads/peekProduct';
import type { ProductRow } from '../types';

/**
 * Build a `patch(partial)` callback that is stable across renders and composes
 * the required full-row payload at write time using peekProduct. Caller does
 * not subscribe to product columns — re-renders only happen when productId
 * changes.
 */
export function usePatchProduct(productId: number) {
  return useCallback((partial: Partial<ProductRow>) => {
    const p = peekProduct(productId);
    if (!p) return;
    execute(updateProduct({ ...p, id: productId, ...partial }));
  }, [productId]);
}
