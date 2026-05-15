import { randomUUID } from 'node:crypto';
import pg from 'pg';
import {
  createDb,
  createLogger,
  createRedis,
  loadConfig,
  migrate,
  type Config,
  type Db,
  type Redis,
} from '@synapse/shared';
import { buildApp } from '../../apps/api/src/app.ts';
import { startWorker } from '../../apps/worker/src/worker.ts';

const BASE_DB_URL = process.env.DATABASE_URL ?? 'postgres://synapse:synapse@localhost:5432/synapse';
export const TEST_DB_URL = BASE_DB_URL.replace(/\/[^/]+$/, '/synapse_test');
export const TEST_REDIS_URL =
  (process.env.REDIS_URL ?? 'redis://localhost:6379').replace(/\/\d*$/, '') + '/15';
export const MAILPIT_URL = process.env.MAILPIT_URL ?? 'http://localhost:8025';

export const testEnv = (overrides: Record<string, string> = {}) => ({
  ...process.env,
  NODE_ENV: 'test',
  LOG_LEVEL: 'warn',
  DATABASE_URL: TEST_DB_URL,
  REDIS_URL: TEST_REDIS_URL,
  CLAIM_IDLE_MS: '1500',
  BACKOFF_BASE_MS: '50',
  BACKOFF_MAX_MS: '400',
  MAX_ATTEMPTS: '3',
  RATE_LIMIT_PER_MINUTE: '100000',
  ...overrides,
});

export const testConfig = (overrides: Record<string, string> = {}): Config =>
  loadConfig(testEnv(overrides));

/** Fresh database + Redis db for a test file. */
export async function resetInfra(): Promise<{ db: Db; redis: Redis }> {
  const admin = new pg.Client({ connectionString: BASE_DB_URL });
  await admin.connect();
  await admin.query('CREATE DATABASE synapse_test').catch((err: { code?: string }) => {
    if (err.code !== '42P04') throw err; // already exists
  });
  await admin.end();

  const db = createDb(TEST_DB_URL);
  await migrate(db);
  await db.query('TRUNCATE deliveries, events, preferences');
  const redis = createRedis(TEST_REDIS_URL);
  await redis.flushdb();
  return { db, redis };
}

export async function startApi(db: Db, config = testConfig()) {
  const redis = createRedis(config.REDIS_URL);
  const sub = createRedis(config.REDIS_URL);
  const app = await buildApp({ config, db, redis, sub, state: { shuttingDown: false } });
  const url = await app.listen({ port: 0, host: '127.0.0.1' });
  return {
    url,
    close: async () => {
      await app.close();
      await Promise.all([redis.quit(), sub.quit()]);
    },
  };
}

export async function startTestWorker(db: Db, config = testConfig(), name = `w-${randomUUID()}`) {
  const redis = createRedis(config.REDIS_URL);
  const stop = await startWorker({ config, db, redis, log: createLogger('worker', 'warn'), name });
  return async () => {
    await stop();
    await redis.quit();
  };
}

export async function api<T = unknown>(
  base: string,
  path: string,
  body?: unknown,
  method?: string,
) {
  const res = await fetch(base + path, {
    method: method ?? (body === undefined ? 'GET' : 'POST'),
    headers: body === undefined ? {} : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, body: (await res.json()) as T };
}

export interface Received {
  id: string;
  eventId: string;
  title: string;
}

/** A browser-like inbox client: collects every notification frame pushed to it. */
export async function connectInbox(base: string, userId: string) {
  const ws = new WebSocket(
    `${base.replace('http', 'ws')}/v1/ws?userId=${encodeURIComponent(userId)}`,
  );
  const received: Received[] = [];
  await new Promise<void>((resolve, reject) => {
    ws.addEventListener('message', (e) => {
      const msg = JSON.parse(String(e.data));
      if (msg.type === 'ready') resolve();
      if (msg.type === 'notification') received.push(msg.notification);
    });
    ws.addEventListener('error', () => reject(new Error('ws error')));
  });
  return { ws, received, ack: (id: string) => ws.send(JSON.stringify({ type: 'ack', id })) };
}

export async function waitFor<T>(
  fn: () => Promise<T> | T,
  { timeout = 15_000, interval = 50, message = 'condition' } = {},
): Promise<Exclude<T, false | null | undefined | 0 | ''>> {
  const deadline = Date.now() + timeout;
  for (;;) {
    const v = await fn();
    if (v) return v as Exclude<T, false | null | undefined | 0 | ''>;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${message}`);
    await new Promise((r) => setTimeout(r, interval));
  }
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
export const uid = (prefix: string) => `${prefix}-${randomUUID().slice(0, 8)}`;

export const orderDelivered = (userId: string, id = randomUUID()) => ({
  id,
  type: 'order.delivered',
  data: { userId, orderId: `ORD-${id.slice(0, 6)}` },
});

export async function countDeliveries(db: Db, where: string, params: unknown[]) {
  const { rows } = await db.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM deliveries WHERE ${where}`,
    params,
  );
  return rows[0]!.n;
}
