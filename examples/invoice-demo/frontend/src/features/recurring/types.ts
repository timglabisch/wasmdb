/**
 * Full row shape for a `recurring_invoices` row as consumed by UpdateRecurring
 * (partial patches composed via peekRecurring).
 */
export interface RecurringRow {
  customer_id: number;
  template_name: string;
  interval_unit: string;
  interval_value: number;
  next_run: string;
  last_run: string;
  enabled: number;
  status_template: string;
  notes_template: string;
}

export interface RecurringPositionRow {
  id: number;
  position_nr: number;
  description: string;
  quantity: number;
  unit_price: number;
  tax_rate: number;
  unit: string;
  item_number: string;
  discount_pct: number;
}
