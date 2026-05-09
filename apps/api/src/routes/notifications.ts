import { z } from 'zod';
import { CHANNELS, DELIVERY_STATUSES } from '@synapse/shared';
import type { Route } from '../app.ts';
import { userParams } from './preferences.ts';

const limit = z.coerce.number().int().min(1).max(500).default(50);

const COLUMNS = `id, event_id AS "eventId", user_id AS "userId", channel, status, title, body, attempts,
  last_error AS "lastError", created_at AS "createdAt", sent_at AS "sentAt", delivered_at AS "deliveredAt"`;

export const notificationRoutes: Route = (app, { db }) => {
  /** The in-app inbox: what the live feed backfills from on (re)connect. */
  app.get('/v1/users/:userId/notifications', async (req) => {
    const { userId } = userParams.parse(req.params);
    const q = z.object({ limit }).parse(req.query);
    const { rows } = await db.query(
      `SELECT ${COLUMNS} FROM deliveries
       WHERE user_id = $1 AND channel = 'in_app' AND status IN ('sent', 'delivered')
       ORDER BY created_at DESC LIMIT $2`,
      [userId, q.limit],
    );
    return rows;
  });

  /** Delivery status tracking across all channels. */
  app.get('/v1/deliveries', async (req) => {
    const q = z
      .object({
        userId: z.string().optional(),
        eventId: z.string().optional(),
        channel: z.enum(CHANNELS).optional(),
        status: z.enum(DELIVERY_STATUSES).optional(),
        limit,
      })
      .parse(req.query);
    const { rows } = await db.query(
      `SELECT ${COLUMNS} FROM deliveries
       WHERE ($1::text IS NULL OR user_id = $1) AND ($2::text IS NULL OR event_id = $2)
         AND ($3::text IS NULL OR channel = $3) AND ($4::text IS NULL OR status = $4)
       ORDER BY created_at DESC LIMIT $5`,
      [q.userId ?? null, q.eventId ?? null, q.channel ?? null, q.status ?? null, q.limit],
    );
    return rows;
  });

  app.get('/v1/deliveries/stats', async () => {
    const { rows } = await db.query<{ channel: string; status: string; count: number }>(
      'SELECT channel, status, count(*)::int AS count FROM deliveries GROUP BY 1, 2 ORDER BY 1, 2',
    );
    return rows;
  });
};
