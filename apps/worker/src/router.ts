import {
  catalog,
  enqueueDelivery,
  getPreferencesMany,
  render,
  type Channel,
  type Preferences,
  type StreamEvent,
} from '@synapse/shared';
import type { WorkerContext } from './context.ts';
import { eventsRouted } from './metrics.ts';

const reachable = (p: Preferences, channel: Channel) =>
  p.channels[channel] &&
  (channel !== 'email' || !!p.email) &&
  (channel !== 'webhook' || (!!p.webhookUrl && !!p.webhookSecret));

/**
 * Router stage: resolves an event's recipients and opted-in channels, renders the template and
 * records one `deliveries` row per (event, user, channel). The unique constraint on that triple is
 * the dedup: republishing or redelivering an event cannot create a second notification.
 */
export async function routeEvent(ctx: WorkerContext, event: StreamEvent): Promise<void> {
  const route = catalog[event.type];
  const parsed = route?.schema.safeParse(event.data);
  if (!route || !parsed?.success) {
    // Validated at ingestion, so this only happens if the catalog changed underneath a backlog.
    ctx.log.error({ eventId: event.id, type: event.type }, 'unroutable event dropped');
    return;
  }
  const data = parsed.data;
  const recipients = route.recipients(data);
  const prefs = await getPreferencesMany(ctx.db, recipients);
  const title = render(route.template.title, data);
  const body = render(route.template.body, data);

  const rows = recipients.flatMap((userId) =>
    route.channels
      .filter((c) => reachable(prefs.get(userId)!, c))
      .map((channel) => ({ userId, channel })),
  );

  const client = await ctx.db.connect();
  let deliveries: { id: string; status: string }[];
  try {
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO events (id, type, data, occurred_at, received_at)
       VALUES ($1, $2, $3, $4, to_timestamp($5 / 1000.0)) ON CONFLICT (id) DO NOTHING`,
      [
        event.id,
        event.type,
        data,
        event.occurredAt ?? new Date(event.receivedAt).toISOString(),
        event.receivedAt,
      ],
    );
    // DO UPDATE (a no-op write) rather than DO NOTHING so existing rows are returned too: if a
    // previous attempt crashed after committing but before enqueueing, we must still enqueue them.
    ({ rows: deliveries } = await client.query(
      `INSERT INTO deliveries (event_id, user_id, channel, title, body)
       SELECT $1, u, c, $4, $5 FROM unnest($2::text[], $3::text[]) AS t(u, c)
       ON CONFLICT (event_id, user_id, channel) DO UPDATE SET event_id = EXCLUDED.event_id
       RETURNING id, status`,
      [event.id, rows.map((r) => r.userId), rows.map((r) => r.channel), title, body],
    ));
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  // Only still-queued rows need work; a duplicate enqueue is harmless (the delivery stage is idempotent).
  await Promise.all(
    deliveries.filter((d) => d.status === 'queued').map((d) => enqueueDelivery(ctx.redis, d.id)),
  );
  eventsRouted.inc({ type: event.type });
}
