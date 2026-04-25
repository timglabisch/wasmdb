import { peekQuery } from '../../../wasm.ts';

export interface ActionPosition {
  position_nr: number;
  description: string;
  quantity: number;
  unit_price: number;
  tax_rate: number;
  product_id: string;
  item_number: string;
  unit: string;
  discount_pct: number;
  cost_price: number;
  position_type: string;
}

/** One-shot non-reactive read of an invoice's positions. */
export function peekPositions(invoiceId: string): ActionPosition[] {
  const rows = peekQuery(
    `SELECT positions.id, positions.position_nr, positions.description, positions.quantity, ` +
    `positions.unit_price, positions.tax_rate, positions.product_id, positions.item_number, ` +
    `positions.unit, positions.discount_pct, positions.cost_price, positions.position_type ` +
    `FROM positions WHERE positions.invoice_id = UUID '${invoiceId}' ORDER BY positions.position_nr`,
  );
  return rows.map((r) => ({
    position_nr: r[1] as number,
    description: r[2] as string,
    quantity: r[3] as number,
    unit_price: r[4] as number,
    tax_rate: r[5] as number,
    product_id: r[6] as string,
    item_number: r[7] as string,
    unit: r[8] as string,
    discount_pct: r[9] as number,
    cost_price: r[10] as number,
    position_type: r[11] as string,
  }));
}
