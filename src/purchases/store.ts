import { ConflictException } from '@nestjs/common';
import { createHash, randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import {
  assertPurchaseCorrelation, canonicalJson, PurchaseProtocolError, PurchaseRequest, PurchaseResult,
} from './contract';

type JournalRow = {
  request: PurchaseRequest;
  result: PurchaseResult | null;
};

export class PurchaseStore {
  constructor(private readonly pool: Pool) {}

  async migrate(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS purchase_requests (
        request_id text PRIMARY KEY,
        idempotency_hash text UNIQUE,
        request jsonb NOT NULL,
        result jsonb,
        last_sent_at timestamptz,
        created_at timestamptz NOT NULL DEFAULT now()
      );
      CREATE INDEX IF NOT EXISTS purchase_requests_pending
        ON purchase_requests (last_sent_at, created_at) WHERE result IS NULL;
    `);
  }

  async create(type: string, payload: Record<string, unknown>, key?: string): Promise<JournalRow> {
    const generatesOrderId = type === 'create_draft' && payload.order_id === undefined;
    const wirePayload = generatesOrderId ? { ...payload, order_id: randomUUID() } : payload;
    const request: PurchaseRequest = {
      request_id: randomUUID(),
      type,
      version: 1,
      occurred_at: new Date().toISOString(),
      payload: wirePayload,
    };
    const keyHash = key === undefined
      ? null
      : createHash('sha256').update(key).digest('hex');
    const inserted = await this.pool.query<JournalRow>(`
      INSERT INTO purchase_requests (request_id, idempotency_hash, request)
      VALUES ($1, $2, $3)
      ON CONFLICT (idempotency_hash) DO NOTHING
      RETURNING request, result
    `, [request.request_id, keyHash, request]);
    if (inserted.rowCount === 1) {
      return inserted.rows[0];
    }
    const existing = await this.pool.query<JournalRow>(
      'SELECT request, result FROM purchase_requests WHERE idempotency_hash = $1',
      [keyHash],
    );
    const row = existing.rows[0];
    const comparablePayload = row ? { ...row.request.payload } : {};
    if (generatesOrderId) delete comparablePayload.order_id;
    if (!row || row.request.type !== type ||
        canonicalJson(comparablePayload) !== canonicalJson(payload)) {
      throw new ConflictException('La clave de idempotencia ya identifica otra solicitud.');
    }
    return row;
  }

  async get(requestId: string): Promise<JournalRow | undefined> {
    const result = await this.pool.query<JournalRow>(
      'SELECT request, result FROM purchase_requests WHERE request_id = $1',
      [requestId],
    );
    return result.rows[0];
  }

  async pending(): Promise<PurchaseRequest[]> {
    const result = await this.pool.query<{ request: PurchaseRequest }>(`
      SELECT request FROM purchase_requests
      WHERE result IS NULL
        AND (last_sent_at IS NULL OR last_sent_at < now() - interval '5 seconds')
      ORDER BY last_sent_at NULLS FIRST, created_at
      LIMIT 50
    `);
    return result.rows.map((row) => row.request);
  }

  async sent(requestId: string): Promise<void> {
    await this.pool.query(
      'UPDATE purchase_requests SET last_sent_at = now() WHERE request_id = $1',
      [requestId],
    );
  }

  async record(result: PurchaseResult): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const existing = await client.query<JournalRow>(
        'SELECT request, result FROM purchase_requests WHERE request_id = $1 FOR UPDATE',
        [result.request_id],
      );
      const row = existing.rows[0];
      if (!row) {
        throw new PurchaseProtocolError('Resultado sin solicitud correlacionada.');
      }
      assertPurchaseCorrelation(row.request, result);
      if (row.result && canonicalJson(row.result) !== canonicalJson(result)) {
        throw new PurchaseProtocolError('Resultado incompatible con el resultado ya persistido.');
      }
      await client.query(
        'UPDATE purchase_requests SET result = $2 WHERE request_id = $1',
        [result.request_id, result],
      );
      await client.query('COMMIT');
    } catch (error) {
      try {
        await client.query('ROLLBACK');
      } catch {
        // Preserve the original failure when a broken connection also prevents rollback.
      }
      throw error;
    } finally {
      client.release();
    }
  }

  async retry(requestId: string): Promise<void> {
    await this.pool.query(
      'UPDATE purchase_requests SET last_sent_at = NULL WHERE request_id = $1 AND result IS NULL',
      [requestId],
    );
  }
}
