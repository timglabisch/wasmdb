import type { UpdateProduct } from '@/generated/UpdateProduct';

export type ProductRow = Omit<UpdateProduct, 'id'>;
