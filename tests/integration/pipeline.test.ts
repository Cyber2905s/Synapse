import { createServer, type Server } from 'node:http';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { KEYS, verifySignature, type Db, type Redis } from '@synapse/shared';
import {
  MAILPIT_URL,
  api,
  connectInbox,
  countDeliveries,
  orderDelivered,
  resetInfra,
  sleep,
  startApi,
  startTestWorker,
  testConfig,
  uid,
  waitFor,
} from './helpers.ts';

let db: Db;
let redis: Redis;
let apiA: Awaited<ReturnType<typeof startApi>>;
let apiB: Awaited<ReturnType<typeof startApi>>;
let stopWorker: () => Promise<void>;

beforeAll(async () => {
  ({ db, redis } = await resetInfra());
  apiA = await startApi(db);
  apiB = await startApi(db);
  stopWorker = await startTestWorker(db);
});

afterAll(async () => {
  await stopWorker();
  await apiA.close();
  await apiB.close();
  await redis.quit();
  await db.end();
});

const statusOf = async (eventId: string, channel: string) =>
  (
    await db.query<{ status: string; attempts: number; last_error: string | null }>(
      'SELECT status, attempts, last_error FROM deliveries WHERE event_id = $1 AND channel = $2',
      [eventId, channel],
    )
  ).rows[0];

describe('ingestion', () => {
  it('rejects invalid payloads and unknown types', async () => {
    expect(
      (await api(apiA.url, '/v1/events', { id: 'x', type: 'order.shipped', data: {} })).status,
    ).toBe(400);
    expect((await api(apiA.url, '/v1/events', { id: 'x', type: 'nope', data: {} })).status).toBe(
      422,
    );
    expect((await api(apiA.url, '/v1/events', { type: 'order.delivered' })).status).toBe(400);
  });
});

describe('end-to-end delivery', () => {
  it('pushes in-app over WebSocket, emails via SMTP, and tracks status through to delivered', async () => {
    const user = uid('alice');
    const email = `${user}@example.com`;
    const inbox = await connectInbox(apiA.url, user);
    const event = orderDelivered(user);

    expect((await api(apiA.url, '/v1/events', event)).status).toBe(202);
    const n = await waitFor(() => inbox.received[0], { message: 'ws notification' });
    expect(n.eventId).toBe(event.id);
    expect(n.title).toBe(`Order ${event.data.orderId} delivered`);

    inbox.ack(n.id);
    await waitFor(async () => (await statusOf(event.id, 'in_app'))?.status === 'delivered', {
      message: 'ack',
    });

    await waitFor(async () => (await statusOf(event.id, 'email'))?.status === 'sent', {
      message: 'email',
    });
    const mail = await waitFor(async () => {
      const r = await fetch(
        `${MAILPIT_URL}/api/v1/search?query=${encodeURIComponent(`to:${email}`)}`,
      );
      return ((await r.json()) as { messages: { Subject: string }[] }).messages[0];
    });
    expect(mail.Subject).toBe(`Order ${event.data.orderId} delivered`);
    inbox.ws.close();
  });

  it('fans out across API instances through Redis pub/sub', async () => {
    const user = uid('fanout');
    const inbox = await connectInbox(apiA.url, user); // socket on instance A
    const event = orderDelivered(user);
    await api(apiB.url, '/v1/events', event); // produced via instance B
    await waitFor(() => inbox.received.length === 1, { message: 'cross-instance push' });
    inbox.ws.close();
  });

  it('dedups by event id: republishing creates no extra notifications', async () => {
    const user = uid('dup');
    const inbox = await connectInbox(apiA.url, user);
    const event = orderDelivered(user);
    for (let i = 0; i < 5; i++) await api(i % 2 ? apiB.url : apiA.url, '/v1/events', event);

    await waitFor(() => inbox.received.length >= 1);
    await sleep(750);
    expect(inbox.received).toHaveLength(1);
    expect(await countDeliveries(db, 'event_id = $1', [event.id])).toBe(2); // in_app + email
    inbox.ws.close();
  });
});

describe('preferences', () => {
  it('skips opted-out channels', async () => {
    const user = uid('optout');
    await api(apiA.url, `/v1/users/${user}/preferences`, { channels: { email: false } }, 'PUT');
    const event = orderDelivered(user);
    await api(apiA.url, '/v1/events', event);
    await waitFor(async () => (await statusOf(event.id, 'in_app'))?.status === 'sent');
    expect(await statusOf(event.id, 'email')).toBeUndefined();
  });

  it('holds email during quiet hours but still delivers in-app', async () => {
    const user = uid('quiet');
    const hh = (d: number) => String((new Date().getUTCHours() + d + 24) % 24).padStart(2, '0');
    await api(
      apiA.url,
      `/v1/users/${user}/preferences`,
      { quietHours: { start: `${hh(-1)}:00`, end: `${hh(2)}:00`, timezone: 'UTC' } },
      'PUT',
    );
    const event = orderDelivered(user);
    await api(apiA.url, '/v1/events', event);
    await waitFor(async () => (await statusOf(event.id, 'in_app'))?.status === 'sent');
    const { rows } = await db.query(
      'SELECT id FROM deliveries WHERE event_id = $1 AND channel = $2',
      [event.id, 'email'],
    );
    const score = await waitFor(() => redis.zscore(KEYS.delayed, rows[0].id), {
      message: 'deferred',
    });
    expect(Number(score)).toBeGreaterThan(Date.now() + 60 * 60_000);
    expect((await statusOf(event.id, 'email'))?.status).toBe('queued');
  });

  it('rate limits per user and defers the excess instead of dropping it', async () => {
    // Avoid straddling a window boundary, which would legitimately allow a second burst.
    if (new Date().getSeconds() > 50) await sleep((61 - new Date().getSeconds()) * 1000);
    const limited = await startTestWorker(db, testConfig({ RATE_LIMIT_PER_MINUTE: '3' }));
    await stopWorker(); // only the limited worker consumes during this test
    try {
      const user = uid('rl');
      await api(apiA.url, `/v1/users/${user}/preferences`, { channels: { email: false } }, 'PUT');
      for (let i = 0; i < 6; i++) await api(apiA.url, '/v1/events', orderDelivered(user));
      await waitFor(async () => (await countDeliveries(db, 'user_id = $1', [user])) === 6);
      await sleep(1000);
      expect(await countDeliveries(db, `user_id = $1 AND status = 'sent'`, [user])).toBe(3);
      expect(await countDeliveries(db, `user_id = $1 AND status = 'queued'`, [user])).toBe(3);
    } finally {
      await limited();
      stopWorker = await startTestWorker(db);
    }
  });
});

describe('webhooks, retries and the DLQ', () => {
  let server: Server;
  let failing = true;
  const received: { deliveryId: string; verified: boolean }[] = [];
  let secret = '';

  beforeAll(async () => {
    server = createServer((req, res) => {
      let raw = '';
      req.on('data', (c) => (raw += c));
      req.on('end', () => {
        if (failing) return res.writeHead(500).end();
        received.push({
          deliveryId: String(req.headers['x-synapse-delivery']),
          verified: verifySignature(secret, raw, String(req.headers['x-synapse-signature'])),
        });
        res.writeHead(204).end();
      });
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  });
  afterAll(() => new Promise<void>((r) => server.close(() => r())));

  it('retries with backoff, dead-letters after MAX_ATTEMPTS, then replays successfully', async () => {
    const user = uid('hook');
    const port = (server.address() as { port: number }).port;
    const prefs = await api<{ webhookSecret: string }>(
      apiA.url,
      `/v1/users/${user}/preferences`,
      {
        channels: { in_app: false, email: false, webhook: true },
        webhookUrl: `http://127.0.0.1:${port}/hook`,
      },
      'PUT',
    );
    secret = prefs.body.webhookSecret;
    expect(secret).toMatch(/^[0-9a-f]{48}$/);

    const event = {
      id: uid('evt'),
      type: 'payment.failed',
      data: { userId: user, amount: 10, currency: 'USD', reason: 'expired card' },
    };
    await api(apiA.url, '/v1/events', event);

    const dead = await waitFor(
      async () => {
        const s = await statusOf(event.id, 'webhook');
        return s?.status === 'failed' && s;
      },
      { message: 'dead-letter' },
    );
    expect(dead.attempts).toBe(3);
    expect(dead.last_error).toBe('webhook responded 500');

    const dlq = await api<{
      size: number;
      entries: { entryId: string; delivery: { eventId: string } }[];
    }>(apiA.url, '/v1/dlq');
    const entry = dlq.body.entries.find((e) => e.delivery.eventId === event.id)!;
    expect(entry).toBeDefined();

    failing = false;
    const replay = await api<{ replayed: number }>(apiA.url, '/v1/dlq/replay', {
      entryIds: [entry.entryId],
    });
    expect(replay.body.replayed).toBe(1);

    await waitFor(async () => (await statusOf(event.id, 'webhook'))?.status === 'delivered', {
      message: 'replay',
    });
    expect(received).toHaveLength(1);
    expect(received[0]!.verified).toBe(true);
    expect((await api<{ size: number }>(apiA.url, '/v1/dlq')).body.size).toBe(0);

    // Replaying the same (now removed) entry again is a no-op.
    expect(
      (await api<{ replayed: number }>(apiA.url, '/v1/dlq/replay', { entryIds: [entry.entryId] }))
        .body.replayed,
    ).toBe(0);
  });
});
