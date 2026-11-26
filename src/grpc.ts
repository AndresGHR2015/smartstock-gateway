import { Injectable, OnModuleDestroy } from '@nestjs/common';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { join } from 'node:path';
import { config } from './config';

type Response = Record<string, any>;
type UnaryClient = grpc.Client & Record<string, any>;

function createClient(proto: string, service: string, address: string): UnaryClient {
  const path = join(process.cwd(), 'vendor/smartstock-infra/contracts/proto', proto);
  const definition = protoLoader.loadSync(path, { keepCase: true, enums: String, defaults: true });
  const root = grpc.loadPackageDefinition(definition) as any;
  const constructor = root.smartstock[service].v1[`${service === 'inventory' ? 'Inventory' : 'Sales'}Service`];
  return new constructor(address, grpc.credentials.createInsecure()) as UnaryClient;
}

@Injectable()
export class GrpcClients implements OnModuleDestroy {
  private readonly inventory = createClient('inventory/v1/inventory.proto', 'inventory', config.inventoryAddress);
  private readonly sales = createClient('sales/v1/sales.proto', 'sales', config.salesAddress);

  private call(client: UnaryClient, method: string, request: object, timeoutMs: number): Promise<Response> {
    return new Promise((resolve, reject) => {
      client[method](request, { deadline: new Date(Date.now() + timeoutMs) }, (error: grpc.ServiceError | null, response: Response) => {
        if (error) reject(error);
        else resolve(response);
      });
    });
  }

  getProduct(sku: string): Promise<Response> {
    return this.call(this.inventory, 'GetProduct', { sku }, config.inventoryTimeoutMs);
  }

  getSale(saleId: string): Promise<Response> {
    return this.call(this.sales, 'GetSale', { sale_id: saleId }, config.salesTimeoutMs);
  }

  createSale(input: { saleId: string; sku: string; quantity: number }): Promise<Response> {
    return this.call(this.sales, 'CreateSale', {
      sale_id: input.saleId,
      sku: input.sku,
      quantity: input.quantity,
    }, config.salesTimeoutMs);
  }

  onModuleDestroy(): void {
    this.inventory.close();
    this.sales.close();
  }
}
