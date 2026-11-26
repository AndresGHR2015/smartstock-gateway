import { Module } from '@nestjs/common';
import { ApolloDriver, ApolloDriverConfig } from '@nestjs/apollo';
import { GraphQLModule } from '@nestjs/graphql';
import { join } from 'node:path';
import { Gateway } from './gateway';
import { GrpcClients } from './grpc';
import { GatewayResolver } from './resolver';
import { PurchasesController } from './purchases/controller';
import { PurchasesService } from './purchases/service';

@Module({
  imports: [
    GraphQLModule.forRoot<ApolloDriverConfig>({
      driver: ApolloDriver,
      typePaths: [join(process.cwd(), 'vendor/smartstock-infra/contracts/graphql/schema.graphql')],
      playground: false,
    }),
  ],
  controllers: [PurchasesController],
  providers: [GrpcClients, Gateway, GatewayResolver, PurchasesService],
})
export class AppModule {}
