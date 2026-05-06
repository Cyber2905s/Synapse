import type { Db } from './db.ts';
import { KEYS, enqueueDelivery, toEntries, type Redis } from './redis.ts';

export interface DlqEntry {
  entryId: string;
  deliveryId: string;
  error: string;
  failedAt: number;
}

export async function listDlq(redis: Redis, count = 100): Promise<DlqEntry[]> {
  const raw = (await redis.xrevrange(KEYS.dlq, '+', '-', 'COUNT', count)) as [string, string[]][];
  return toEntries(raw).map(({ id, fields }) => ({
    entryId: id,
    deliveryId: fields.deliveryId ?? '',
    error: fields.error ?? '',
    failedAt: Number(fields.failedAt),
  }));
}

/**
 * Re-queues dead deliveries with a fresh retry budget. Ordering is crash-safe: the row is reset and
 * re-enqueued before the DLQ entry is removed, and the `status = 'failed'` guard makes a repeated
 * replay of the same entry a no-op.
 */
export async function replayDlq(db: Db, redis: Redis, entryIds?: string[]): Promise<number> {
  const entries = (await listDlq(redis, 10_000)).filter(
    (e) => !entryIds || entryIds.includes(e.entryId),
  );
  let replayed = 0;
  for (const e of entries) {
    const { rowCount } = await db.query(
      `UPDATE deliveries SET status = 'queued', attempts = 0, last_error = NULL, updated_at = now()
       WHERE id = $1 AND status = 'failed'`,
      [e.deliveryId],
    );
    if (rowCount) {
      await enqueueDelivery(redis, e.deliveryId);
      replayed++;
    }
    await redis.xdel(KEYS.dlq, e.entryId);
  }
  return replayed;
}
