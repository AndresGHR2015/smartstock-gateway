import { Args, Mutation, Query, Resolver } from '@nestjs/graphql';
import { GraphQLError } from 'graphql';
import { Gateway } from './gateway';
import { PurchasesService } from './purchases/service';

@Resolver()
export class GatewayResolver {
  constructor(private readonly gateway: Gateway, private readonly purchases: PurchasesService) {}

  @Query()
  product(@Args('sku') sku: string) {
    return this.gateway.product(sku);
  }

  @Query()
  sale(@Args('id') id: string) {
    return this.gateway.sale(id);
  }

  @Query()
  inventoryOverview() {
    return this.gateway.inventoryOverview();
  }

  @Query()
  salesOverview() {
    return this.gateway.salesOverview();
  }

  @Query()
  async purchasesOverview() {
    try {
      const { pending_count, open_order_count, needs } = await this.purchases.overview();
      return {
        pendingCount: pending_count,
        openOrderCount: open_order_count,
        needs: needs.map(({ event_id, sku, occurred_at, status }) => ({
          eventId: event_id, sku, occurredAt: occurred_at, status,
        })),
      };
    } catch {
      throw new GraphQLError('UPSTREAM_UNAVAILABLE', { extensions: { code: 'UPSTREAM_UNAVAILABLE' } });
    }
  }

  @Mutation()
  createSale(@Args('input') input: { saleId: string; sku: string; quantity: number }) {
    return this.gateway.createSale(input);
  }
}
