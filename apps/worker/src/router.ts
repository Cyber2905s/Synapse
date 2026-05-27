import {
  KEYS,
  catalog,
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
 * Router stage: resolves each event's recipients and opted-in channels, renders the template and
 * records one `deliveries` row per (event, user, channel). The unique constraint on that triple is
 * the dedup: republishing or redelivering an event cannot create a second notification.
 *
 * A whole stream batch is written in a single statement, so it costs one commit (one fsync) rather
 * than one per event, and is atomic without an explicit transaction.
 */
export async function routeEvents(ctx: WorkerContext, batch: StreamEvent[]): Promise<void> {
  const routable = new Map<string, { event: StreamEvent; data: Record<string, unknown> }>();
  for (const event of batch) {
    const route = catalog[event.type];
    const parsed = route?.schema.safeParse(event.data);
    if (!route || !parsed?.success) {
      // Validated at ingestion, so this only happens if the catalog changed underneath a backlog.
      ctx.log.error({ eventId: event.id, type: event.type }, 'unroutable event dropped');
      continue;
    }
    // A republished event can share a batch with its original; one statement can't upsert the
    // same row twice, so keep the first.
    if (!routable.has(event.id)) routable.set(event.id, { event, data: parsed.data });
  }
  if (!routable.size) return;

  const items = [...routable.values()].map(({ event, data }) => {
    const route = catalog[event.type]!;
    return { event, data, route, recipients: route.recipients(data) };
  });
  const prefs = await getPreferencesMany(ctx.db, [...new Set(items.flatMap((i) => i.recipients))]);

  const rows = items.flatMap(({ event, data, route, recipients }) => {
    const title = render(route.template.title, data);
    const body = render(route.template.body, data);
    return recipients.flatMap((userId) =>
      route.channels
        .filter((c) => reachable(prefs.get(userId)!, c))
        .map((channel) => ({ eventId: event.id, userId, channel, title, body })),
    );
  });

  // DO UPDATE (a no-op write) rather than DO NOTHING so existing rows are returned too: if a
  // previous attempt crashed after committing but before enqueueing, they must still be enqueued.
  const { rows: deliveries } = await ctx.db.query<{ id: string; status: string }>(
    `WITH ev AS (
       INSERT INTO events (id, type, data, occurred_at, received_at)
       SELECT id, type, data, occurred_at, to_timestamp(received_ms / 1000.0)
       FROM unnest($1::text[], $2::text[], $3::jsonb[], $4::timestamptz[], $5::float8[])
         AS t(id, type, data, occurred_at, received_ms)
       ON CONFLICT (id) DO NOTHING
     )
     INSERT INTO deliveries (event_id, user_id, channel, title, body)
     SELECT * FROM unnest($6::text[], $7::text[], $8::text[], $9::text[], $10::text[])
     ON CONFLICT (event_id, user_id, channel) DO UPDATE SET event_id = EXCLUDED.event_id
     RETURNING id, status`,
    [
      items.map((i) => i.event.id),
      items.map((i) => i.event.type),
      items.map((i) => JSON.stringify(i.data)),
      items.map((i) => i.event.occurredAt ?? new Date(i.event.receivedAt).toISOString()),
      items.map((i) => i.event.receivedAt),
      rows.map((r) => r.eventId),
      rows.map((r) => r.userId),
      rows.map((r) => r.channel),
      rows.map((r) => r.title),
      rows.map((r) => r.body),
    ],
  );

  // Only still-queued rows need work; a duplicate enqueue is harmless (the delivery stage is idempotent).
  const queued = deliveries.filter((d) => d.status === 'queued');
  if (queued.length) {
    const pipeline = ctx.redis.pipeline();
    for (const d of queued) pipeline.xadd(KEYS.deliveries, '*', 'deliveryId', d.id);
    const failed = (await pipeline.exec())?.find(([err]) => err);
    if (failed) throw failed[0]; // leave the batch unacked so it is re-routed
  }
  for (const i of items) eventsRouted.inc({ type: i.event.type });
}
