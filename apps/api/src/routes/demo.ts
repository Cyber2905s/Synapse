import { z } from 'zod';
import { SIGNATURE_HEADER, getPreferences, verifySignature } from '@synapse/shared';
import type { Route } from '../app.ts';

const SINK = 'synapse:demo:webhooks';
const FAILING = 'synapse:demo:webhook-failing';

/**
 * A stand-in "customer" webhook endpoint so the demo is self-contained: it verifies the HMAC
 * signature, records what arrived, and can be switched into failure mode to exercise retries/DLQ.
 */
export const demoRoutes: Route = (app, { db, redis }) => {
  // Keep the raw body: signatures are computed over the exact bytes sent.
  app.register(async (scope) => {
    scope.addContentTypeParser('application/json', { parseAs: 'string' }, (_req, body, done) =>
      done(null, body),
    );

    scope.post('/demo/webhook-sink', async (req, reply) => {
      const raw = req.body as string;
      const payload = JSON.parse(raw) as { userId?: string };
      const deliveryId = req.headers['x-synapse-delivery'];
      if ((await redis.get(FAILING)) === '1')
        return reply.code(503).send({ error: 'sink in failure mode' });
      const prefs = payload.userId ? await getPreferences(db, payload.userId) : null;
      const signature = String(req.headers[SIGNATURE_HEADER] ?? '');
      const verified =
        !!prefs?.webhookSecret && verifySignature(prefs.webhookSecret, raw, signature);
      await redis
        .multi()
        .lpush(SINK, JSON.stringify({ deliveryId, verified, receivedAt: Date.now(), payload }))
        .ltrim(SINK, 0, 49)
        .exec();
      return reply.code(verified ? 200 : 401).send({ verified });
    });
  });

  app.get('/demo/webhook-sink', async () => ({
    failing: (await redis.get(FAILING)) === '1',
    received: (await redis.lrange(SINK, 0, 49)).map((s) => JSON.parse(s)),
  }));

  app.put('/demo/webhook-sink/mode', async (req) => {
    const { failing } = z.object({ failing: z.boolean() }).parse(req.body);
    await redis.set(FAILING, failing ? '1' : '0');
    return { failing };
  });
};
