import { MessageList } from '../components/MessageList';
import { OnlineUserList } from '../components/OnlineUserList';
import { RoomList } from '../components/RoomList';
import { UserBadge } from '../components/UserBadge';
import { SEED } from '../seed';
import {
  BtnRenameUnknownUser,
  BtnRenameUserA,
  BtnRenameUserASame,
  BtnRenameUserB,
  BtnRenameUsersAAndB,
  BtnStatusUserABusy,
  BtnStatusUserCOnline,
} from './buttons';
import type { Scenario } from './types';

const StandaloneBadges = () => (
  <section className="panel">
    <h2>Standalone user badges</h2>
    <div className="row">
      <UserBadge id={SEED.users.A} ctx="standalone" />
      <UserBadge id={SEED.users.B} ctx="standalone" />
      <UserBadge id={SEED.users.C} ctx="standalone" />
    </div>
  </section>
);

const USER_BADGE_SQL = `SELECT users.id, users.name, users.status
FROM users
WHERE REACTIVE(users.id = UUID '<user-id>')`;

export const userScenarios: Scenario[] = [
  {
    id: 'user-no-op-write',
    category: 'users',
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
  },

  {
    id: 'user-unknown-id',
    category: 'users',
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
  },

  {
    id: 'user-batch',
    category: 'users',
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
  },

  {
    id: 'user-multi-instance',
    category: 'users',
    title: 'Same user, multiple instances: every UserBadge:A ticks',
    summary:
      'Alice appears in three places — owner badge of R1, owner badge of R3, author badge of M1. UpdateUserName(A) must fan out to all three contexts, regardless of context tag.',
    expectations: [
      'Click "Rename Alice" → all three Alice badges tick:',
      '— UserBadge:Alice@room:R1 (owner)',
      '— UserBadge:Alice@room:R3 (owner)',
      '— UserBadge:Alice@msg:M1 (author)',
      'Bob/Carol badges stay quiet.',
    ],
    shouldRender: [`*UserBadge:${SEED.users.A}*`],
    shouldStayQuiet: [`*UserBadge:${SEED.users.B}*`, `*UserBadge:${SEED.users.C}*`],
    subscriptions: [
      {
        component: 'UserBadge:* (× many instances)',
        sql: USER_BADGE_SQL,
        note: 'Each <UserBadge> instance subscribes independently — the engine fans out to all instances bound to the same user-id.',
      },
    ],
    Body: () => (
      <>
        <RoomList />
        <section className="panel">
          <h2>R1 messages (M1 is by Alice)</h2>
          <MessageList roomId={SEED.rooms.R1} />
        </section>
        <div className="row">
          <BtnRenameUserA />
        </div>
      </>
    ),
  },

  {
    id: 'user-status-change',
    category: 'users',
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
  },

  {
    id: 'user-online-filter',
    category: 'users',
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
  },
];
