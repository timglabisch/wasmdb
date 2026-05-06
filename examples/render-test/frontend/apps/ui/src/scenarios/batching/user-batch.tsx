import type { Scenario } from '@wasmdb/scenarios';
import { SEED } from '../../seed';
import { BtnRenameUsersAAndB } from '../buttons';
import { StandaloneBadges } from '../components/StandaloneBadges';
import { USER_BADGE_SQL } from '../components/queries';

export const userBatch: Scenario = {
  id: 'user-batch',
  category: 'batching',
  title: 'Two writes in one tick fan out to exactly those two subscriber sets',
  summary:
    'Two UpdateUserName commands fired synchronously in the same tick. Both Alice\'s and Bob\'s badges must render; Carol\'s must stay quiet. Verifies multi-write isolation.',
  expectations: [
    'Click "Rename Alice + Bob" → Alice badges and Bob badges tick.',
    'Carol badges stay quiet.',
  ],
  shouldRender: [`*UserBadge:${SEED.users.A}*`, `*UserBadge:${SEED.users.B}*`],
  shouldStayQuiet: [`*UserBadge:${SEED.users.C}*`],
  subscriptions: [{ component: 'UserBadge:*', sql: USER_BADGE_SQL }],
  Body: () => (
    <>
      <StandaloneBadges />
      <div className="row">
        <BtnRenameUsersAAndB />
      </div>
    </>
  ),
};
