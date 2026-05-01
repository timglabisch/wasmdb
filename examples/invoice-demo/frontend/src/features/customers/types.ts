import type { UpdateCustomer } from '@/generated/UpdateCustomer';
import type { UpdateContact } from '@/generated/UpdateContact';
import type { UpdateSepaMandate } from '@/generated/UpdateSepaMandate';

export type CustomerRow = Omit<UpdateCustomer, 'id'>;
export type ContactRow = Omit<UpdateContact, 'id'>;
export type SepaMandateRow = Omit<UpdateSepaMandate, 'id'>;
