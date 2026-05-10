import { z } from 'zod';
import { KEYS, listDlq, replayDlq } from '@synapse/shared';
import type { Route } from '../app.ts';

export const dlqRoutes: Route = (app, { db, redis }) => {
  app.get('/v1/dlq', async () => {
    const entries = await listDlq(redis);
    const ids = entries.map((e) => e.deliveryId);
    const { rows } = await db.query(
      `SELECT id, event_id AS "eventId", user_id AS "userId", channel, title, attempts
       FROM deliveries WHERE id = ANY($1::uuid[])`,
      [ids],
    );
    const byId = new Map(rows.map((r) => [r.id, r]));
    return {
      size: await redis.xlen(KEYS.dlq),
      entries: entries.map((e) => ({ ...e, delivery: byId.get(e.deliveryId) ?? null })),
    };
  });

  /** Replays the given DLQ entries, or all of them when `entryIds` is omitted. */
  app.post('/v1/dlq/replay', async (req) => {
    const { entryIds } = z
      .object({ entryIds: z.array(z.string()).max(1000).optional() })
      .parse(req.body ?? {});
    return { replayed: await replayDlq(db, redis, entryIds) };
  });
};
