import type { UpdateInvoiceHeader } from '@/generated/UpdateInvoiceHeader';

export type InvoiceRow = Omit<UpdateInvoiceHeader, 'id'> & {
  customer_id: string | null;
};
