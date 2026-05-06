import type { Scenario } from '@wasmdb/scenarios';
import { RoomList } from '../../components/RoomList';
import { BtnRenameUnknownUser } from '../buttons';
import { StandaloneBadges } from '../components/StandaloneBadges';
import { USER_BADGE_SQL } from '../components/queries';

export const userUnknownId: Scenario = {
  id: 'user-unknown-id',
  category: 'filters',
  title: 'Update of unknown id does not match any subscriber',
  summary:
    'Updating a row that no component subscribes to must not tick anything. Echo-server still performs an INSERT/UPDATE roundtrip; the dirty-cycle runs but no per-row predicate matches.',
  expectations: [
    'Click "Rename unknown user" → no UserBadge ticks.',
    'No RoomRow ticks (the unknown user is not an owner anywhere).',
  ],
  shouldStayQuiet: ['UserBadge:*', 'RoomRow:*'],
  subscriptions: [
    {
      component: 'UserBadge:*',
      sql: USER_BADGE_SQL,
      note: 'Predicate fixes user-id at mount; an UPDATE on a different id never matches.',
    },
  ],
  Body: () => (
    <>
      <StandaloneBadges />
      <RoomList />
      <div className="row">
        <BtnRenameUnknownUser />
      </div>
    </>
  ),
};
