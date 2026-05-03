export type EntityTypeFilter =
  | 'all'
  | 'customer'
  | 'invoice'
  | 'payment'
  | 'recurring'
  | 'product'
  | 'sepa';

export interface EntityFilterOption {
  value: EntityTypeFilter;
  label: string;
}

export const ENTITY_FILTERS: EntityFilterOption[] = [
  { value: 'all', label: 'Alle' },
  { value: 'customer', label: 'Kunden' },
  { value: 'invoice', label: 'Rechnungen' },
  { value: 'payment', label: 'Zahlungen' },
  { value: 'recurring', label: 'Serien' },
  { value: 'product', label: 'Produkte' },
  { value: 'sepa', label: 'SEPA' },
];

export const PAGE_SIZE = 200;
