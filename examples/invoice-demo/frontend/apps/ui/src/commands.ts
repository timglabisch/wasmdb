import * as client from '@wasmdb/client';
import type { InvoiceCommand } from 'invoice-demo-generated/InvoiceCommand';

export const execute = (cmd: InvoiceCommand) => client.execute(cmd);
export const executeOnStream = (id: number, cmd: InvoiceCommand) => client.executeOnStream(id, cmd);
