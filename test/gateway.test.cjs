require('reflect-metadata');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Gateway } = require('../dist/gateway');
const { GatewayResolver } = require('../dist/resolver');

const pending = {
  sale_id: ' sale-1 ', sku: 'SKU', quantity: 2,
  status: 'PENDING', rejection_reason: 'REJECTION_REASON_UNSPECIFIED',
};
const input = { saleId: ' sale-1 ', sku: 'SKU', quantity: 2 };

test('resolver delegates without changing identifiers', async () => {
  const resolver = new GatewayResolver(new Gateway({
    createSale: async (received) => {
      assert.deepEqual(received, input);
      return { result: 'ACCEPTED', sale: pending };
    },
  }));
  assert.deepEqual(await resolver.createSale(input), {
    outcome: 'ACCEPTED',
    sale: { saleId: ' sale-1 ', sku: 'SKU', quantity: 2, status: 'PENDING', rejectionReason: null },
  });
});

test('business outcomes and invalid input remain distinct', async () => {
  const gateway = new Gateway({
    createSale: async () => ({ result: 'SALE_ID_CONFLICT' }),
    getProduct: async () => ({ result: 'INVALID_SKU' }),
    getSale: async () => ({ result: 'NOT_FOUND' }),
  });
  assert.equal((await gateway.createSale(input)).outcome, 'SALE_ID_CONFLICT');
  assert.equal((await gateway.createSale({ ...input, quantity: 0 })).outcome, 'INVALID_ARGUMENT');
  assert.equal(await gateway.sale('missing'), null);
  await assert.rejects(gateway.product('SKU'), (error) => error.extensions.code === 'INVALID_ARGUMENT');
});

test('technical and unknown results are never reported as success', async () => {
  const gateway = new Gateway({
    createSale: async () => { throw new Error('deadline'); },
    getProduct: async () => { throw new Error('unavailable'); },
    getSale: async () => ({ result: 'RESULT_UNSPECIFIED' }),
  });
  assert.deepEqual(await gateway.createSale(input), { outcome: 'UNKNOWN', sale: null });
  await assert.rejects(gateway.product('SKU'), (error) => error.extensions.code === 'UPSTREAM_UNAVAILABLE');
  await assert.rejects(gateway.sale('id'), (error) => error.extensions.code === 'UPSTREAM_UNAVAILABLE');
  const malformed = new Gateway({ createSale: async () => ({ result: 'ACCEPTED', sale: { ...pending, status: 'SALE_STATUS_UNSPECIFIED' } }) });
  assert.equal((await malformed.createSale(input)).outcome, 'UNKNOWN');
});

test('rejected sales include only their authoritative rejection reason', async () => {
  const gateway = new Gateway({
    getSale: async () => ({
      result: 'FOUND',
      sale: { ...pending, status: 'REJECTED', rejection_reason: 'INSUFFICIENT_STOCK' },
    }),
  });
  assert.equal((await gateway.sale('id')).rejectionReason, 'INSUFFICIENT_STOCK');
});
