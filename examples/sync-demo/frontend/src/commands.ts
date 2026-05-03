import * as client from '@wasmdb/client';
import type { UserCommand } from './generated/UserCommand';

export const execute = (cmd: UserCommand) => client.execute(cmd);
export const executeOnStream = (id: number, cmd: UserCommand) => client.executeOnStream(id, cmd);

let counter = 0;
export const nextId = (): number => Date.now() * 1000 + (counter++ % 1000);
