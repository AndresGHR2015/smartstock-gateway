import {
  Body, Controller, Get, Headers, HttpCode, Param, Post, UnauthorizedException,
} from '@nestjs/common';
import { timingSafeEqual } from 'node:crypto';
import { PurchasesService } from './service';

export function authorizePurchases(authorization?: string): void {
  const token = process.env.PURCHASES_INTERNAL_TOKEN;
  if (!token || !authorization) {
    throw new UnauthorizedException();
  }
  const expected = Buffer.from(`Bearer ${token}`);
  const received = Buffer.from(authorization);
  if (expected.length !== received.length || !timingSafeEqual(expected, received)) {
    throw new UnauthorizedException();
  }
}

@Controller('internal/purchases/requests')
export class PurchasesController {
  constructor(private readonly purchases: PurchasesService) {}

  @Post()
  @HttpCode(202)
  submit(
    @Headers('authorization') authorization: string | undefined,
    @Headers('idempotency-key') key: string | undefined,
    @Body() body: unknown,
  ) {
    authorizePurchases(authorization);
    return this.purchases.submit(body, key);
  }

  @Get(':id')
  get(@Headers('authorization') authorization: string | undefined, @Param('id') id: string) {
    authorizePurchases(authorization);
    return this.purchases.get(id);
  }

  @Post(':id/retry')
  @HttpCode(202)
  retry(@Headers('authorization') authorization: string | undefined, @Param('id') id: string) {
    authorizePurchases(authorization);
    return this.purchases.retry(id);
  }
}
