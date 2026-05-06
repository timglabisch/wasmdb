import type { Scenario } from '@wasmdb/scenarios';
import { OnlineUserList } from '../../components/OnlineUserList';
import { BtnStatusUserABusy, BtnStatusUserCOnline } from '../buttons';

export const userOnlineFilter: Scenario = {
  id: 'user-online-filter',
  category: 'filters',
  title: 'Filter boundary: row crosses WHERE predicate',
  summary:
    '`<OnlineUserList>` filters `WHERE status = \'online\'`. When a user transitions across the predicate (online → busy or vice versa), the list\'s membership changes and it must re-render.',
  expectations: [
    'Initial seed: Alice + Bob online, Carol away → list shows two names.',
    'Click "Alice → busy" → OnlineUserList re-renders, Alice drops out.',
    'Click "Carol → online" → OnlineUserList re-renders, Carol joins.',
  ],
  shouldRender: ['OnlineUserList'],
  subscriptions: [
    {
      component: 'OnlineUserList',
      sql: `SELECT users.id, users.name
FROM users
WHERE REACTIVE(users.status = 'online')
ORDER BY users.name`,
      note: 'Predicate-based REACTIVE: any change that affects whether a row matches `status=online` must re-fire.',
    },
  ],
  Body: () => (
    <>
      <OnlineUserList />
      <div className="row">
        <BtnStatusUserABusy />
        <BtnStatusUserCOnline />
      </div>
    </>
  ),
};
