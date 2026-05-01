import type { UpdateRecurring } from '@/generated/UpdateRecurring';
import type { UpdateRecurringPosition } from '@/generated/UpdateRecurringPosition';

export type RecurringRow = Omit<UpdateRecurring, 'id'> & {
  customer_id: string;
  last_run: string;
};

export type RecurringPositionRow = UpdateRecurringPosition & {
  position_nr: number;
};
