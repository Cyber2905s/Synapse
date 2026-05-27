import {
  KEYS,
  backoffDelay,
  defaultPreferences,
  msUntilQuietEnds,
  preferencesFromRow,
  type Channel,
  type Preferences,
  type PreferencesRow,
} from '@synapse/shared';
import { sendEmail, sendWebhook } from './channels.ts';
import type { WorkerContext } from './context.ts';
import { deliveries, deliveryLatency, deliveryRetries } from './metrics.ts';

export interface DeliveryRow {
  id: string;
  event_id: string;
  user_id: string;
  channel: Channel;
  status: string;
  title: string;
  body: string;
  attempts: number;
  lease_until: Date | null;
  created_at: Date;
  event_type: string;
  event_data: unknown;
  received_at: Date;
  prefs: PreferencesRow | null;
}

/** Parks a delivery in the delayed set; the promoter re-enqueues it once due. */
const defer = (ctx: WorkerContext, id: string, ms: number) =>
  ctx.redis.zadd(KEYS.delayed, Date.now() + Math.max(ms, 0), id);

/** Fixed one-minute window per user, shared across channels. */
async function overRateLimit(ctx: WorkerContext, userId: string): Promise<number> {
  const window = Math.floor(Date.now() / 60_000);
  const key = KEYS.rateLimit(userId, window);
  const [[, count]] = (await ctx.redis.multi().incr(key).expire(key, 120).exec()) as [
    [null, number],
  ];
  return count > ctx.config.RATE_LIMIT_PER_MINUTE ? (window + 1) * 60_000 - Date.now() : 0;
}

/** How long this delivery must wait (quiet hours, then rate limit), or 0 to send now. */
async function holdFor(ctx: WorkerContext, d: DeliveryRow, prefs: Preferences): Promise<number> {
  // In-app is silent and pull-based, so quiet hours only hold back the interruptive channels.
  const quietMs =
    d.channel === 'in_app' || !prefs.quietHours
      ? 0
      : msUntilQuietEnds(prefs.quietHours, new Date());
  return quietMs || overRateLimit(ctx, d.user_id);
}

/**
 * Delivery stage, one stream batch at a time. Idempotent under redelivery: anything not `queued`
 * is already settled. In-app deliveries are claimed by one conditional update per batch (one
 * commit, one fsync); email and webhook take a per-delivery lease so only one worker is mid-send.
 */
export async function deliverBatch(ctx: WorkerContext, ids: string[]): Promise<void> {
  const { rows } = await ctx.db.query<DeliveryRow>(
    `SELECT d.*, e.type AS event_type, e.data AS event_data, e.received_at, row_to_json(p) AS prefs
     FROM deliveries d
     JOIN events e ON e.id = d.event_id
     LEFT JOIN preferences p ON p.user_id = d.user_id
     WHERE d.id = ANY($1::uuid[]) AND d.status = 'queued'`,
    [[...new Set(ids)]],
  );

  const inApp: DeliveryRow[] = [];
  const external: Promise<void>[] = [];
  await Promise.all(
    rows.map(async (d) => {
      const prefs = d.prefs ? preferencesFromRow(d.prefs) : defaultPreferences(d.user_id);
      const waitMs = await holdFor(ctx, d, prefs);
      if (waitMs > 0) {
        deliveries.inc({ channel: d.channel, outcome: 'deferred' });
        // Small jitter so a burst deferred to the same boundary doesn't stampede back.
        await defer(ctx, d.id, waitMs + Math.random() * 1000);
      } else if (d.channel === 'in_app') {
        inApp.push(d);
      } else {
        external.push(deliverExternal(ctx, d, prefs));
      }
    }),
  );

  await Promise.all([deliverInApp(ctx, inApp), ...external]);
}

/**
 * No lease needed: the conditional UPDATE is the atomic claim and its row locks serialise racers.
 * Publishing *inside* that transaction means a crash after COMMIT can never skip a push, and a
 * crash between PUBLISH and COMMIT re-pushes the same notification ids, which clients collapse.
 * Failures here are infrastructure (Postgres/Redis down): thrown, so the batch stays pending for
 * redelivery instead of burning the retry budget.
 */
async function deliverInApp(ctx: WorkerContext, batch: DeliveryRow[]): Promise<void> {
  if (!batch.length) return;
  const client = await ctx.db.connect();
  let sent: { id: string; userId: string }[];
  try {
    await client.query('BEGIN');
    ({ rows: sent } = await client.query(
      `UPDATE deliveries SET status = 'sent', attempts = attempts + 1, sent_at = now(), updated_at = now()
       WHERE id = ANY($1::uuid[]) AND status = 'queued'
       RETURNING id, event_id AS "eventId", user_id AS "userId", channel, status, title, body,
         created_at AS "createdAt", sent_at AS "sentAt"`,
      [batch.map((d) => d.id)],
    ));
    if (sent.length) {
      const pipeline = ctx.redis.pipeline();
      for (const n of sent) pipeline.publish(KEYS.userChannel(n.userId), JSON.stringify(n));
      const failed = (await pipeline.exec())?.find(([err]) => err);
      if (failed) throw failed[0];
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
  const receivedAt = new Map(batch.map((d) => [d.id, d.received_at.getTime()]));
  for (const n of sent) {
    deliveries.inc({ channel: 'in_app', outcome: 'sent' });
    deliveryLatency.observe({ channel: 'in_app' }, (Date.now() - receivedAt.get(n.id)!) / 1000);
  }
}

async function deliverExternal(
  ctx: WorkerContext,
  d: DeliveryRow,
  prefs: Preferences,
): Promise<void> {
  // Take the lease. Losing the race means another worker is mid-send (or it just settled).
  const lease = await ctx.db.query<{ attempts: number }>(
    `UPDATE deliveries SET attempts = attempts + 1, lease_until = now() + $2 * interval '1 millisecond',
       updated_at = now()
     WHERE id = $1 AND status = 'queued' AND (lease_until IS NULL OR lease_until < now())
     RETURNING attempts`,
    [d.id, ctx.config.CLAIM_IDLE_MS],
  );
  if (!lease.rows[0]) {
    if (d.lease_until) await defer(ctx, d.id, d.lease_until.getTime() - Date.now() + 100);
    return;
  }
  const attempt = lease.rows[0].attempts;

  try {
    await (d.channel === 'email' ? sendEmail : sendWebhook)(ctx, d, prefs);
    // SMTP acceptance is "sent" (no delivery receipt); a webhook 2xx is proof of delivery.
    const status = d.channel === 'webhook' ? 'delivered' : 'sent';
    await ctx.db.query(
      `UPDATE deliveries SET status = $2, sent_at = now(),
         delivered_at = CASE WHEN $2 = 'delivered' THEN now() END, lease_until = NULL, updated_at = now()
       WHERE id = $1`,
      [d.id, status],
    );
    deliveries.inc({ channel: d.channel, outcome: status });
    deliveryLatency.observe({ channel: d.channel }, (Date.now() - d.received_at.getTime()) / 1000);
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);
    if (attempt >= ctx.config.MAX_ATTEMPTS) {
      // DLQ entry first: a crash between the two steps yields a duplicate DLQ entry (replay is
      // idempotent), never a failed delivery missing from the DLQ.
      await ctx.redis.xadd(
        KEYS.dlq,
        '*',
        'deliveryId',
        d.id,
        'error',
        error,
        'failedAt',
        String(Date.now()),
      );
      await ctx.db.query(
        `UPDATE deliveries SET status = 'failed', last_error = $2, lease_until = NULL, updated_at = now() WHERE id = $1`,
        [d.id, error],
      );
      deliveries.inc({ channel: d.channel, outcome: 'dead' });
      ctx.log.warn({ deliveryId: d.id, attempt, error }, 'delivery dead-lettered');
    } else {
      await ctx.db.query(
        `UPDATE deliveries SET last_error = $2, lease_until = NULL, updated_at = now() WHERE id = $1`,
        [d.id, error],
      );
      const delay = backoffDelay(attempt, ctx.config.BACKOFF_BASE_MS, ctx.config.BACKOFF_MAX_MS);
      await defer(ctx, d.id, delay);
      deliveries.inc({ channel: d.channel, outcome: 'retry' });
      deliveryRetries.inc({ channel: d.channel });
      ctx.log.info({ deliveryId: d.id, attempt, delay, error }, 'delivery failed; retry scheduled');
    }
  }
}
