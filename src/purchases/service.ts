import {
  BadRequestException, ForbiddenException, Injectable, Logger,
  NotFoundException, OnModuleDestroy, OnModuleInit, ServiceUnavailableException,
} from '@nestjs/common';
import { Kafka, Consumer, Producer, logLevel } from 'kafkajs';
import { Pool } from 'pg';
import { setTimeout as delay } from 'node:timers/promises';
import {
  PurchaseRequest, PurchaseResult, validPurchaseRequest, validPurchaseResult,
  PurchaseProtocolError,
} from './contract';
import { PurchaseStore } from './store';

function containsNul(value: unknown): boolean {
  if (typeof value === 'string') return value.includes('\0');
  if (value !== null && typeof value === 'object') {
    return Object.entries(value).some(([key, item]) => key.includes('\0') || containsNul(item));
  }
  return false;
}

@Injectable()
export class PurchasesService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PurchasesService.name);
  private pool?: Pool;
  private store?: PurchaseStore;
  private producer?: Producer;
  private consumer?: Consumer;
  private timer?: ReturnType<typeof setInterval>;
  private dispatchTask?: Promise<void>;
  private healthy = false;
  private readonly shutdown = new AbortController();

  async onModuleInit(): Promise<void> {
    const brokers = process.env.KAFKA_BOOTSTRAP_SERVERS;
    if (!brokers) return;
    if (!process.env.PURCHASES_DATABASE_URL || !process.env.PURCHASES_INTERNAL_TOKEN ||
        process.env.PURCHASES_INTERNAL_TOKEN.length < 16) {
      throw new Error('Compras requiere PURCHASES_DATABASE_URL y PURCHASES_INTERNAL_TOKEN (mínimo 16 caracteres).');
    }
    this.pool = new Pool({
      connectionString: process.env.PURCHASES_DATABASE_URL,
      connectionTimeoutMillis: 5000,
      query_timeout: 5000,
      statement_timeout: 5000,
      lock_timeout: 5000,
    });
    this.pool.on('error', () => this.logger.error('Conexión inactiva de PostgreSQL perdida.'));
    this.store = new PurchaseStore(this.pool);
    await this.store.migrate();
    const kafka = new Kafka({
      clientId: 'smartstock-gateway',
      brokers: brokers.split(','),
      logLevel: logLevel.WARN,
    });
    this.producer = kafka.producer({ allowAutoTopicCreation: false });
    this.consumer = kafka.consumer({
      groupId: process.env.PURCHASES_KAFKA_GROUP_ID || 'smartstock-gateway-purchases',
      allowAutoTopicCreation: false,
      retry: { restartOnFailure: async (error) => !(error instanceof PurchaseProtocolError) },
    });
    await this.producer.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: 'purchases.results.v1', fromBeginning: true });
    this.consumer.on(this.consumer.events.CRASH, ({ payload }) => {
      this.healthy = false;
      if (payload.restart) {
        this.logger.warn('Kafka no disponible; el consumidor de Compras se reconectará.');
      } else {
        this.logger.error('Consumidor de Compras detenido; requiere revisión.');
      }
    });
    this.consumer.on(this.consumer.events.GROUP_JOIN, () => {
      if (!this.shutdown.signal.aborted) {
        this.healthy = true;
        this.startDispatch();
      }
    });
    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message, heartbeat }) => {
        let value: unknown;
        try {
          value = JSON.parse(message.value?.toString('utf8') || 'null');
        } catch {
          throw new PurchaseProtocolError('Resultado de Compras no es JSON.');
        }
        if (!validPurchaseResult(value)) {
          throw new PurchaseProtocolError('Resultado de Compras inválido; offset no confirmado.');
        }
        const result = value as PurchaseResult;
        if (message.key?.toString('utf8') !== result.request_id) {
          throw new PurchaseProtocolError('La clave Kafka no coincide con request_id.');
        }
        await this.recordWithRetry(result, heartbeat);
        await heartbeat();
        await this.consumer!.commitOffsets([{
          topic, partition, offset: (BigInt(message.offset) + 1n).toString(),
        }]);
      },
    });
    this.healthy = true;
    this.timer = setInterval(() => this.startDispatch(), 1000);
    this.startDispatch();
  }

  private async recordWithRetry(result: PurchaseResult, heartbeat: () => Promise<void>): Promise<void> {
    while (!this.shutdown.signal.aborted) {
      try {
        await this.store!.record(result);
        return;
      } catch (error) {
        if (error instanceof PurchaseProtocolError) throw error;
        const code = (error as { code?: string }).code || '';
        const transient = code.startsWith('08') ||
          ['ECONNREFUSED', 'ECONNRESET', 'EPIPE', 'ETIMEDOUT', '57P01', '57P02', '57P03',
            '40001', '40P01', '53300', '57014', '55P03'].includes(code) ||
          (error instanceof Error && error.message === 'Query read timeout');
        const disconnected = error instanceof Error &&
          ['Connection terminated unexpectedly', 'Connection terminated',
            'Client has encountered a connection error and is not queryable'].includes(error.message);
        if (!transient && !disconnected) throw error;
        this.logger.error('PostgreSQL no disponible; resultado y offset siguen pendientes.');
        await heartbeat();
        await delay(1000, undefined, { signal: this.shutdown.signal });
      }
    }
    throw new Error('Gateway detenido antes de persistir el resultado.');
  }

  private startDispatch(): void {
    if (!this.dispatchTask && this.healthy) {
      this.dispatchTask = this.dispatch()
        .catch(() => this.logger.error('No se pudo enviar a Compras; la solicitud sigue pendiente.'))
        .finally(() => { this.dispatchTask = undefined; });
    }
  }

  private async dispatch(): Promise<void> {
    for (const request of await this.store!.pending()) {
      await this.producer!.send({
        topic: 'purchases.requests.v1',
        acks: -1,
        messages: [{ key: request.request_id, value: JSON.stringify(request) }],
      });
      await this.store!.sent(request.request_id);
    }
  }

  private requireStore(): PurchaseStore {
    if (!this.store) {
      throw new ServiceUnavailableException('La integración con Compras no está configurada.');
    }
    return this.store;
  }

  async submit(body: unknown, key?: string) {
    const store = this.requireStore();
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      throw new BadRequestException('Se requieren type y payload.');
    }
    const input = body as Record<string, unknown>;
    const request = {
      request_id: 'validation',
      type: input.type,
      version: 1,
      occurred_at: new Date().toISOString(),
      payload: input.payload,
    };
    if (input.type === 'create_draft' && input.payload &&
        typeof input.payload === 'object' && !Array.isArray(input.payload)) {
      const payload = input.payload as Record<string, unknown>;
      if (payload.order_id === undefined) {
        request.payload = { ...payload, order_id: 'generated-after-validation' };
      }
    }
    if (Object.keys(input).some((field) => !['type', 'payload'].includes(field)) ||
        !validPurchaseRequest(request) ||
        containsNul(body) ||
        (key !== undefined && (!key.trim() || key.length > 256))) {
      throw new BadRequestException('Solicitud de Compras inválida.');
    }
    const validated = request as PurchaseRequest;
    if (validated.type === 'decide_order' &&
        (process.env.PURCHASES_ENVIRONMENT !== 'local' ||
         process.env.PURCHASES_ALLOW_LOCAL_DECISIONS !== 'true' ||
         validated.payload.actor_id !== (process.env.PURCHASES_LOCAL_ACTOR_ID || 'local-operator'))) {
      throw new ForbiddenException('Las decisiones humanas requieren autorización local explícita.');
    }
    const row = await store.create(
      validated.type, input.payload as Record<string, unknown>, key,
    );
    this.startDispatch();
    return this.view(row.request, row.result);
  }

  async get(requestId: string) {
    const row = await this.requireStore().get(requestId);
    if (!row) throw new NotFoundException('Solicitud no encontrada.');
    return this.view(row.request, row.result);
  }

  async retry(requestId: string) {
    const row = await this.requireStore().get(requestId);
    if (!row) throw new NotFoundException('Solicitud no encontrada.');
    await this.requireStore().retry(requestId);
    this.startDispatch();
    return this.view(row.request, row.result);
  }

  private view(request: PurchaseRequest, result: PurchaseResult | null) {
    return {
      request_id: request.request_id,
      type: request.type,
      order_id: request.payload.order_id ?? null,
      status: result ? 'COMPLETED' : 'PENDING',
      result,
    };
  }

  async onModuleDestroy(): Promise<void> {
    this.healthy = false;
    this.shutdown.abort();
    if (this.timer) clearInterval(this.timer);
    await this.dispatchTask;
    await this.consumer?.disconnect();
    await this.producer?.disconnect();
    await this.pool?.end();
  }
}
