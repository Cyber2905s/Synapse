import { Redis } from 'ioredis';

export { Redis };

export const KEYS = {
  /** Domain events published by producers. Consumed by the router stage. */
  events: 'synapse:events',
  /** One entry per (event, user, channel) delivery attempt. Consumed by the delivery stage. */
  deliveries: 'synapse:deliveries',
  /** Deliveries that exhausted their retries. */
  dlq: 'synapse:dlq',
  /** Sorted set of delivery ids scored by the epoch ms at which they become due again. */
  delayed: 'synapse:delayed',
  /** Pub/sub channel carrying in-app notifications to whichever API instance holds the socket. */
  userChannel: (userId: string) => `synapse:user:${userId}`,
  rateLimit: (userId: string, window: number) => `synapse:rl:${userId}:${window}`,
} as const;

export const GROUPS = { router: 'router', delivery: 'delivery' } as const;

export const createRedis = (url: string) => new Redis(url, { maxRetriesPerRequest: null });

export async function ensureGroup(redis: Redis, stream: string, group: string): Promise<void> {
  try {
    await redis.xgroup('CREATE', stream, group, '0', 'MKSTREAM');
  } catch (err) {
    if (!String(err).includes('BUSYGROUP')) throw err;
  }
}

export interface StreamEntry {
  id: string;
  fields: Record<string, string>;
}

/** Converts `[id, [k1, v1, k2, v2]]` pairs; entries deleted while pending come back with null fields. */
export const toEntries = (raw: [string, string[] | null][]): StreamEntry[] =>
  raw.map(([id, flat]) => {
    const fields: Record<string, string> = {};
    for (let i = 0; flat && i < flat.length; i += 2) fields[flat[i]!] = flat[i + 1]!;
    return { id, fields };
  });

// Known limit: streams are never trimmed; add a periodic `XTRIM MINID` below the oldest pending id
// once retention matters (MAXLEN trimming could drop unacked entries).
export const enqueueDelivery = (redis: Redis, deliveryId: string) =>
  redis.xadd(KEYS.deliveries, '*', 'deliveryId', deliveryId);
