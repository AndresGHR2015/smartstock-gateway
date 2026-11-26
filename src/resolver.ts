import { Args, Mutation, Query, Resolver } from '@nestjs/graphql';
import { Gateway } from './gateway';

@Resolver()
export class GatewayResolver {
  constructor(private readonly gateway: Gateway) {}

  @Query()
  product(@Args('sku') sku: string) {
    return this.gateway.product(sku);
  }

  @Query()
  sale(@Args('id') id: string) {
    return this.gateway.sale(id);
  }

  @Mutation()
  createSale(@Args('input') input: { saleId: string; sku: string; quantity: number }) {
    return this.gateway.createSale(input);
  }
}
