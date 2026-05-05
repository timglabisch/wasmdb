import type { ReactNode } from 'react';

export type ScenarioCategory = 'counters' | 'users' | 'rooms' | 'messages' | 'joins' | 'hooks';

export interface Scenario {
  id: string;
  category: ScenarioCategory;
  title: string;
  summary: string;
  expectations: string[];
  Body: () => ReactNode;
}

export const CATEGORY_LABEL: Record<ScenarioCategory, string> = {
  counters: 'Counters',
  users: 'Users',
  rooms: 'Rooms',
  messages: 'Messages',
  joins: 'Joins',
  hooks: 'Hook probes',
};
