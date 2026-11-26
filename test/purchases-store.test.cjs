require('reflect-metadata');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { Pool } = require('pg');
const { PurchaseStore } = require('../dist/purchases/store');

test('PostgreSQL journal deduplicates concurrent accepts, rejects conflicts and retains results', {
  skip: !process.env.TEST_PURCHASES_DATABASE_URL,
}, async (t) => {
  const pool = new Pool({ connectionString: process.env.TEST_PURCHASES_DATABASE_URL });
  t.after(() => pool.end());
  const store = new PurchaseStore(pool);
  await store.migrate();
  const key = randomUUID();
  const submissions = await Promise.all(Array.from(
    { length: 10 },
    () => store.create('get_need', { event_id: 'event-1' }, key),
  ));
  const original = submissions[0].request;
  assert.equal(new Set(submissions.map((row) => row.request.request_id)).size, 1);
  await assert.rejects(
    store.create('get_need', { event_id: 'other-event' }, key),
    (error) => error.getStatus() === 409,
  );
  const result = {
    request_id: original.request_id,
    type: original.type,
    version: 1,
    occurred_at: new Date().toISOString(),
    outcome: 'NOT_FOUND',
    data: null,
  };
  await store.record(result);
  await store.record({ ...result });
  await assert.rejects(store.record({ ...result, outcome: 'FORBIDDEN' }));
  const freshStore = new PurchaseStore(pool);
  assert.deepEqual((await freshStore.get(original.request_id)).result, result);
  assert.deepEqual((await store.create('get_need', { event_id: 'event-1' }, key)).request, original);
});

test('draft identifiers generated in Gateway remain stable on client retries', {
  skip: !process.env.TEST_PURCHASES_DATABASE_URL,
}, async (t) => {
  const pool = new Pool({ connectionString: process.env.TEST_PURCHASES_DATABASE_URL });
  t.after(() => pool.end());
  const store = new PurchaseStore(pool);
  await store.migrate();
  const key = randomUUID();
  const payload = { need_event_id: 'event-1', offer_id: 'quote-a', quantity: 10 };
  const original = await store.create('create_draft', payload, key);
  const replay = await store.create('create_draft', payload, key);
  assert.ok(original.request.payload.order_id);
  assert.deepEqual(replay, original);
});
