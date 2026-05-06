/** Subscription SQL strings shared across scenarios. Used in the
 *  `subscriptions` field of a Scenario to render a "what each component
 *  is subscribed to" reference panel. */

export const MSG_LIST_SQL = `SELECT messages.id
FROM messages
WHERE REACTIVE(messages.room_id = UUID '<room-id>')
ORDER BY messages.created_at`;

export const MSG_ITEM_SQL = `SELECT messages.id, messages.body, messages.author_user_id, messages.created_at
FROM messages
WHERE REACTIVE(messages.id = UUID '<msg-id>')`;

export const MSG_COUNT_SQL = `SELECT COUNT(messages.id)
FROM messages
WHERE REACTIVE(messages.room_id = UUID '<room-id>')`;

export const ROOM_ROW_SQL = `SELECT rooms.id, rooms.name, rooms.owner_user_id
FROM rooms
WHERE REACTIVE(rooms.id = UUID '<room-id>')`;

export const ROOM_LIST_SQL = `SELECT REACTIVE(rooms.id), rooms.id
FROM rooms
ORDER BY rooms.name`;

export const USER_BADGE_SQL = `SELECT users.id, users.name, users.status
FROM users
WHERE REACTIVE(users.id = UUID '<user-id>')`;
