import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import Ajv2020 from 'ajv/dist/2020';
import addFormats from 'ajv-formats';
import { KafkaJSNonRetriableError } from 'kafkajs';

export type PurchaseRequest = {
  request_id: string;
  type: string;
  version: 1;
  occurred_at: string;
  payload: Record<string, unknown>;
};

export type PurchaseResult = {
  request_id: string;
  type: string;
  version: 1;
  occurred_at: string;
  outcome: string;
  data: Record<string, unknown> | null;
};

const ajv = new Ajv2020({ allErrors: true });
addFormats(ajv);

function compileContract(topic: string) {
  const filename = join(
    process.cwd(), 'vendor/smartstock-infra/contracts/events', topic, 'schema.json',
  );
  return ajv.compile(JSON.parse(readFileSync(filename, 'utf8')));
}

export const validPurchaseRequest = compileContract('purchases.requests.v1');
export const validPurchaseResult = compileContract('purchases.results.v1');

export class PurchaseProtocolError extends KafkaJSNonRetriableError {}

export function assertPurchaseCorrelation(request: PurchaseRequest, result: PurchaseResult): void {
  if (request.request_id !== result.request_id || request.type !== result.type) {
    throw new PurchaseProtocolError('Resultado sin solicitud correlacionada.');
  }
  if (result.outcome !== 'OK') return;
  const payload = request.payload;
  const data = result.data!;
  let matches: boolean;
  switch (request.type) {
    case 'get_need':
      matches = data.event_id === payload.event_id;
      break;
    case 'list_needs':
    case 'list_offers': {
      const items = data.items as Record<string, unknown>[];
      matches = items.length <= (payload.limit as number) &&
        items.every((item) => item.sku === payload.sku);
      break;
    }
    case 'get_order':
      matches = data.order_id === payload.order_id;
      break;
    case 'create_draft':
      matches = data.order_id === payload.order_id &&
        data.need_event_id === payload.need_event_id &&
        data.quantity === payload.quantity &&
        (data.offer as Record<string, unknown>).offer_id === payload.offer_id;
      break;
    case 'decide_order':
      matches = data.order_id === payload.order_id &&
        data.revision === (payload.expected_revision as number) + 1 &&
        data.decided_by === payload.actor_id &&
        data.status === (payload.decision === 'APPROVE' ? 'APPROVED' : 'REJECTED');
      break;
    default:
      matches = false;
  }
  if (!matches) {
    throw new PurchaseProtocolError('Los datos del resultado no corresponden a la solicitud.');
  }
}

export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(',')}]`;
  }
  if (value !== null && typeof value === 'object') {
    const record = value as Record<string, unknown>;
    const fields = Object.keys(record).sort().map(
      (key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`,
    );
    return `{${fields.join(',')}}`;
  }
  const serialized = JSON.stringify(value);
  if (serialized === undefined) {
    throw new Error('El valor no es JSON.');
  }
  return serialized;
}
