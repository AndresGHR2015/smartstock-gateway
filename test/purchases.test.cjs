require('reflect-metadata');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  assertPurchaseCorrelation,
  canonicalJson,
  PurchaseProtocolError,
  validPurchaseRequest,
  validPurchaseResult,
} = require('../dist/purchases/contract');
const { authorizePurchases } = require('../dist/purchases/controller');
const { PurchasesService } = require('../dist/purchases/service');

const request = {
  request_id: 'request-1',
  type: 'get_need',
  version: 1,
  occurred_at: '2026-11-25T12:00:00Z',
  payload: { event_id: 'event-1' },
};
const result = {
  request_id: 'request-1',
  type: 'get_need',
  version: 1,
  occurred_at: '2026-11-25T12:00:01Z',
  outcome: 'OK',
  data: { event_id: 'event-1', sku: 'SKU-001', current_stock: 7, threshold: 7 },
};

function setEnv(t, values) {
  const previous = {};
  for (const [key, value] of Object.entries(values)) {
    previous[key] = process.env[key];
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  t.after(() => {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  });
}

test('purchase schemas reject unknown fields, wrong payloads and partial results', () => {
  assert.equal(validPurchaseRequest(request), true);
  assert.equal(validPurchaseResult(result), true);
  assert.equal(validPurchaseRequest({ ...request, type: 'get_order' }), false);
  assert.equal(validPurchaseRequest({ ...request, extra: true }), false);
  assert.equal(validPurchaseRequest({ ...request, request_id: ' ' }), false);
  assert.equal(validPurchaseResult({ ...result, outcome: 'UNKNOWN' }), false);
  assert.equal(validPurchaseResult({ ...result, data: null }), false);
});

test('schema-valid results must still correlate with the submitted arguments', () => {
  assert.doesNotThrow(() => assertPurchaseCorrelation(request, result));
  const wrongNeed = { ...result, data: { ...result.data, event_id: 'another-event' } };
  assert.equal(validPurchaseResult(wrongNeed), true);
  assert.throws(() => assertPurchaseCorrelation(request, wrongNeed));
  assert.throws(() => assertPurchaseCorrelation(request, { ...result, request_id: 'another' }));
});

test('purchase list results cannot contain a different SKU or exceed the requested page size', () => {
  const listRequest = {
    ...request,
    type: 'list_needs',
    payload: { sku: 'SKU-001', limit: 1, cursor: null },
  };
  const page = {
    ...result,
    type: 'list_needs',
    data: { items: [result.data], next_cursor: null },
  };
  assert.doesNotThrow(() => assertPurchaseCorrelation(listRequest, page));
  assert.throws(() => assertPurchaseCorrelation(listRequest, {
    ...page,
    data: { ...page.data, items: [{ ...result.data, sku: 'other-sku' }] },
  }));
  assert.throws(() => assertPurchaseCorrelation(listRequest, {
    ...page,
    data: { ...page.data, items: [result.data, result.data] },
  }));
});

test('decision results must match the actor, intended decision and next revision', () => {
  const decisionRequest = {
    ...request,
    type: 'decide_order',
    payload: {
      order_id: 'order-1',
      expected_revision: 1,
      decision: 'APPROVE',
      actor_id: 'local-operator',
    },
  };
  const decisionResult = {
    ...result,
    type: 'decide_order',
    data: { order_id: 'order-1', status: 'APPROVED', revision: 2, decided_by: 'local-operator' },
  };
  assert.doesNotThrow(() => assertPurchaseCorrelation(decisionRequest, decisionResult));
  for (const mismatch of [
    { revision: 3 },
    { status: 'REJECTED' },
    { decided_by: 'another-actor' },
    { order_id: 'another-order' },
  ]) {
    assert.throws(() => assertPurchaseCorrelation(decisionRequest, {
      ...decisionResult,
      data: { ...decisionResult.data, ...mismatch },
    }));
  }
});

test('canonical request equality ignores object property ordering, not identifier content', () => {
  assert.equal(canonicalJson({ a: 1, b: { z: 2, x: 3 } }), canonicalJson({ b: { x: 3, z: 2 }, a: 1 }));
  assert.notEqual(canonicalJson({ id: ' A ' }), canonicalJson({ id: 'A' }));
});

test('purchase endpoints require the configured bearer credential', (t) => {
  setEnv(t, { PURCHASES_INTERNAL_TOKEN: 'test-internal-token-1234' });
  assert.throws(() => authorizePurchases());
  assert.throws(() => authorizePurchases('Bearer wrong'));
  assert.doesNotThrow(() => authorizePurchases('Bearer test-internal-token-1234'));
});

test('purchase decisions fail closed without local environment and explicit opt-in', async (t) => {
  setEnv(t, {
    PURCHASES_ENVIRONMENT: 'production',
    PURCHASES_ALLOW_LOCAL_DECISIONS: 'true',
  });
  const service = new PurchasesService();
  service.store = { create: async () => assert.fail('decision must not be persisted') };
  const body = {
    type: 'decide_order',
    payload: {
      order_id: 'order-1',
      expected_revision: 1,
      decision: 'APPROVE',
      actor_id: 'local-operator',
    },
  };
  await assert.rejects(service.submit(body), (error) => error.getStatus() === 403);
  process.env.PURCHASES_ENVIRONMENT = 'local';
  process.env.PURCHASES_ALLOW_LOCAL_DECISIONS = 'false';
  await assert.rejects(service.submit(body), (error) => error.getStatus() === 403);
  process.env.PURCHASES_ALLOW_LOCAL_DECISIONS = 'true';
  await assert.rejects(
    service.submit({ ...body, payload: { ...body.payload, actor_id: 'forged-actor' } }),
    (error) => error.getStatus() === 403,
  );
});

test('purchase submission rejects unknown input before persisting or publishing', async () => {
  const service = new PurchasesService();
  service.store = { create: async () => assert.fail('invalid input must not be persisted') };
  await assert.rejects(service.submit({ type: 'get_need', payload: {} }), (error) => error.getStatus() === 400);
});

test('disabled Kafka integration does not break the existing GraphQL application', async () => {
  const service = new PurchasesService();
  await assert.rejects(
    service.submit({ type: 'get_need', payload: { event_id: 'event-1' } }),
    (error) => error.getStatus() === 503,
  );
});

test('transient database failure keeps Kafka membership alive until persistence succeeds', async () => {
  const service = new PurchasesService();
  let attempts = 0;
  let heartbeats = 0;
  service.store = {
    record: async (received) => {
      assert.deepEqual(received, result);
      attempts++;
      if (attempts === 1) {
        const error = new Error('database restarting');
        error.code = 'ECONNREFUSED';
        throw error;
      }
    },
  };
  await service.recordWithRetry(result, async () => { heartbeats++; });
  assert.equal(attempts, 2);
  assert.equal(heartbeats, 1);
});

test('protocol violations are fatal rather than retried as database outages', async () => {
  const service = new PurchasesService();
  let attempts = 0;
  service.store = {
    record: async () => {
      attempts++;
      throw new PurchaseProtocolError('divergent result');
    },
  };
  await assert.rejects(
    service.recordWithRetry(result, async () => assert.fail('no retry heartbeat expected')),
    PurchaseProtocolError,
  );
  assert.equal(attempts, 1);
});
