import type { Scenario } from '@wasmdb/scenarios';
import { SEED } from '../../seed';
import { BtnRenameUserASame } from '../buttons';
import { StandaloneBadges } from '../components/StandaloneBadges';
import { USER_BADGE_SQL } from '../components/queries';

export const userNoOpWrite: Scenario = {
  id: 'user-no-op-write',
  category: 'reactivity',
  title: 'No-op write still re-renders (engine has no value-equality check)',
  summary:
    'Pin the no-op invariant: writing the same value still triggers a dirty-cycle. This is the *current* behavior — the test fences it so a future "skip identical writes" optimization is a deliberate, observable change.',
  expectations: [
    'Click "Rename Alice → Alice" → at least one UserBadge:Alice instance still ticks.',
    'If the engine adds equality-skip in the future, this assertion must flip.',
  ],
  shouldRender: [`*UserBadge:${SEED.users.A}*`],
  shouldStayQuiet: [`*UserBadge:${SEED.users.B}*`, `*UserBadge:${SEED.users.C}*`],
  subscriptions: [{ component: 'UserBadge:*', sql: USER_BADGE_SQL }],
  Body: () => (
    <>
      <StandaloneBadges />
      <div className="row">
        <BtnRenameUserASame />
      </div>
    </>
  ),
};
