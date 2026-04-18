/**
 * Full row shape for a `products` row, as consumed by UpdateProduct plus the
 * display columns. Parallel to InvoiceRow / CustomerRow — this is the
 * caller-side projection returned by peekProduct() and accepted by
 * usePatchProduct() as a Partial.
 */
export interface ProductRow {
  sku: string;
  name: string;
  description: string;
  unit: string;
  unit_price: number;
  tax_rate: number;
  cost_price: number;
  active: number;
}
