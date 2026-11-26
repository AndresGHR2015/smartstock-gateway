import { Injectable } from '@nestjs/common';
import { GraphQLError } from 'graphql';
import { GrpcClients } from './grpc';

type SaleInput = { saleId: string; sku: string; quantity: number };
type WireSale = {
  sale_id: string;
  sku: string;
  quantity: number;
  status: string;
  rejection_reason: string;
};

function validText(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function graphError(code: 'INVALID_ARGUMENT' | 'UPSTREAM_UNAVAILABLE'): GraphQLError {
  return new GraphQLError(code, { extensions: { code } });
}

function mapSale(sale: WireSale | undefined) {
  if (!sale || !validText(sale.sale_id) || !validText(sale.sku) ||
      !Number.isInteger(sale.quantity) || sale.quantity <= 0 || sale.quantity > 2147483647 ||
      !['PENDING', 'CONFIRMED', 'REJECTED'].includes(sale.status)) {
    throw new Error('Invalid sale from upstream');
  }
  const rejected = sale.status === 'REJECTED';
  const validReason = ['PRODUCT_NOT_FOUND', 'INSUFFICIENT_STOCK'].includes(sale.rejection_reason);
  if ((rejected && !validReason) || (!rejected && sale.rejection_reason !== 'REJECTION_REASON_UNSPECIFIED')) {
    throw new Error('Invalid rejection reason from upstream');
  }
  return {
    saleId: sale.sale_id,
    sku: sale.sku,
    quantity: sale.quantity,
    status: sale.status,
    rejectionReason: rejected ? sale.rejection_reason : null,
  };
}

@Injectable()
export class Gateway {
  constructor(private readonly grpc: GrpcClients) {}

  async product(sku: string) {
    if (!validText(sku)) throw graphError('INVALID_ARGUMENT');
    let response;
    try {
      response = await this.grpc.getProduct(sku);
    } catch {
      throw graphError('UPSTREAM_UNAVAILABLE');
    }
    if (response.result === 'NOT_FOUND') return null;
    if (response.result === 'INVALID_SKU') throw graphError('INVALID_ARGUMENT');
    const product = response.product;
    if (response.result !== 'FOUND' || !product || !validText(product.sku) ||
        typeof product.name !== 'string' ||
        !Number.isInteger(product.stock) || product.stock < 0 || product.stock > 2147483647 ||
        !Number.isInteger(product.threshold) || product.threshold < 0 || product.threshold > 2147483647) {
      throw graphError('UPSTREAM_UNAVAILABLE');
    }
    return product;
  }

  async sale(id: string) {
    if (!validText(id)) throw graphError('INVALID_ARGUMENT');
    let response;
    try {
      response = await this.grpc.getSale(id);
    } catch {
      throw graphError('UPSTREAM_UNAVAILABLE');
    }
    if (response.result === 'NOT_FOUND') return null;
    if (response.result === 'INVALID_SALE_ID') throw graphError('INVALID_ARGUMENT');
    try {
      if (response.result !== 'FOUND') throw new Error('Unknown result');
      return mapSale(response.sale);
    } catch {
      throw graphError('UPSTREAM_UNAVAILABLE');
    }
  }

  async createSale(input: SaleInput) {
    if (!validText(input.saleId) || !validText(input.sku) ||
        !Number.isInteger(input.quantity) || input.quantity <= 0 || input.quantity > 2147483647) {
      return { outcome: 'INVALID_ARGUMENT', sale: null };
    }
    try {
      const response = await this.grpc.createSale(input);
      if (response.result === 'INVALID_ARGUMENT' || response.result === 'SALE_ID_CONFLICT') {
        return { outcome: response.result, sale: null };
      }
      if (response.result === 'ACCEPTED') {
        return { outcome: 'ACCEPTED', sale: mapSale(response.sale) };
      }
    } catch {
      // The upstream may have committed even when its response was lost.
    }
    return { outcome: 'UNKNOWN', sale: null };
  }
}
