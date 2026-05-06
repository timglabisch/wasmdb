import type { Scenario } from '@wasmdb/scenarios';
import { SEED } from '../../seed';
import { BtnStatusUserABusy } from '../buttons';
import { StandaloneBadges } from '../components/StandaloneBadges';
import { USER_BADGE_SQL } from '../components/queries';

export const userStatusChange: Scenario = {
  id: 'user-status-change',
  category: 'reactivity',
  title: 'UpdateUserStatus: only the affected user\'s badges tick',
  summary:
    'Status is just another column in `users`. Changing it propagates to all UserBadge instances of that user (badge subscribes to the whole row), and not to other users\' badges.',
  expectations: [
    'Click "Alice → busy" → Alice badges tick (status pill turns "busy").',
    'Bob/Carol badges stay quiet.',
  ],
  shouldRender: [`*UserBadge:${SEED.users.A}*`],
  shouldStayQuiet: [`*UserBadge:${SEED.users.B}*`, `*UserBadge:${SEED.users.C}*`],
  subscriptions: [{ component: 'UserBadge:*', sql: USER_BADGE_SQL }],
  Body: () => (
    <>
      <StandaloneBadges />
      <div className="row">
        <BtnStatusUserABusy />
      </div>
    </>
  ),
};
