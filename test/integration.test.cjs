require('reflect-metadata');
const { test } = require('node:test');
const assert = require('node:assert/strict');
const grpc = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const path = require('node:path');
const { NestFactory } = require('@nestjs/core');

function service(proto, name) {
  const file = path.join(process.cwd(), 'vendor/smartstock-infra/contracts/proto', proto);
  const definition = loader.loadSync(file, { keepCase: true, enums: String, defaults: true });
  return grpc.loadPackageDefinition(definition).smartstock[name].v1;
}

async function startServer(definition, handlers) {
  const server = new grpc.Server();
  server.addService(definition, handlers);
  const port = await new Promise((resolve, reject) => {
    server.bindAsync('127.0.0.1:0', grpc.ServerCredentials.createInsecure(), (error, value) => {
      if (error) reject(error);
      else resolve(value);
    });
  });
  return { server, port };
}

test('GraphQL over HTTP calls gRPC doubles and preserves a pending sale', async (t) => {
  const inventory = await startServer(service('inventory/v1/inventory.proto', 'inventory').InventoryService.service, {
    GetProduct: (call, callback) => callback(null, {
      result: 'FOUND', product: { sku: call.request.sku, name: 'Café', stock: 7, threshold: 2 },
    }),
    DeductStock: (_call, callback) => callback({ code: grpc.status.UNIMPLEMENTED, message: 'not used' }),
  });
  const sales = await startServer(service('sales/v1/sales.proto', 'sales').SalesService.service, {
    CreateSale: (call, callback) => {
      if (call.request.sale_id === 'timeout') return;
      callback(null, { result: 'ACCEPTED', sale: { ...call.request, status: 'PENDING' } });
    },
    GetSale: (call, callback) => callback(null, {
      result: 'FOUND', sale: { sale_id: call.request.sale_id, sku: 'A', quantity: 2, status: 'PENDING' },
    }),
  });
  t.after(() => { inventory.server.forceShutdown(); sales.server.forceShutdown(); });
  process.env.INVENTORY_GRPC_ADDRESS = `127.0.0.1:${inventory.port}`;
  process.env.SALES_GRPC_ADDRESS = `127.0.0.1:${sales.port}`;
  process.env.SALES_GRPC_TIMEOUT_MS = '100';
  const { AppModule } = require('../dist/app.module');
  const app = await NestFactory.create(AppModule, { logger: false });
  t.after(async () => app.close());
  await app.listen(0, '127.0.0.1');
  const url = `${await app.getUrl()}/graphql`;
  async function query(text, variables) {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query: text, variables }),
    });
    return response.json();
  }
  const result = await query(
    'query($sku: String!, $id: ID!) { product(sku: $sku) { sku stock } sale(id: $id) { saleId status } }',
    { sku: 'A', id: 'sale-1' },
  );
  assert.deepEqual(result.data, { product: { sku: 'A', stock: 7 }, sale: { saleId: 'sale-1', status: 'PENDING' } });
  const mutation = await query(
    'mutation($input: CreateSaleInput!) { createSale(input: $input) { outcome sale { saleId status } } }',
    { input: { saleId: 'sale-1', sku: 'A', quantity: 2 } },
  );
  assert.deepEqual(mutation.data, { createSale: { outcome: 'ACCEPTED', sale: { saleId: 'sale-1', status: 'PENDING' } } });
  const timedOut = await query(
    'mutation($input: CreateSaleInput!) { createSale(input: $input) { outcome sale { saleId } } }',
    { input: { saleId: 'timeout', sku: 'A', quantity: 2 } },
  );
  assert.deepEqual(timedOut.data, { createSale: { outcome: 'UNKNOWN', sale: null } });
});
