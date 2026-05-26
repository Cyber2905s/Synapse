import { toEntries, type Logger, type Redis, type StreamEntry } from '@synapse/shared';

export interface ConsumerOptions {
  /** Dedicated connection: XREADGROUP BLOCK ties it up. */
  redis: Redis;
  stream: string;
  group: string;
  consumer: string;
  /** Entries pending longer than this are presumed orphaned by a dead consumer and taken over. */
  claimIdleMs: number;
  batch: number;
  log: Logger;
  /** Receives a whole batch so it can commit once per batch rather than once per entry. */
  handle: (entries: StreamEntry[]) => Promise<void>;
}

/**
 * At-least-once stream consumer. A batch is XACKed only after its handler resolves, so a crash
 * anywhere before that leaves it in the group's pending list, where another consumer's XAUTOCLAIM
 * picks it up once it has been idle for `claimIdleMs`. Handlers must therefore be idempotent.
 */
export function startConsumer(o: ConsumerOptions) {
  let running = true;
  let lastClaim = 0;

  const process = async (entries: StreamEntry[]) => {
    // Entries deleted while pending come back without fields: nothing to do but ack them.
    const live = entries.filter((e) => Object.keys(e.fields).length);
    try {
      if (live.length) await o.handle(live);
      await o.redis.xack(o.stream, o.group, ...entries.map((e) => e.id));
    } catch (err) {
      // Not acked: the batch is reclaimed and retried after claimIdleMs.
      o.log.error(
        { err, stream: o.stream, count: entries.length },
        'batch failed; leaving pending',
      );
    }
  };

  const reclaim = async () => {
    let cursor = '0-0';
    do {
      const [next, raw] = (await o.redis.xautoclaim(
        o.stream,
        o.group,
        o.consumer,
        o.claimIdleMs,
        cursor,
        'COUNT',
        o.batch,
      )) as [string, [string, string[] | null][]];
      if (raw.length) {
        o.log.warn({ stream: o.stream, count: raw.length }, 'reclaimed orphaned entries');
        await process(toEntries(raw));
      }
      cursor = next;
    } while (running && cursor !== '0-0');
  };

  const done = (async () => {
    while (running) {
      try {
        if (Date.now() - lastClaim >= o.claimIdleMs / 2) {
          lastClaim = Date.now();
          await reclaim();
        }
        const res = (await o.redis.xreadgroup(
          'GROUP',
          o.group,
          o.consumer,
          'COUNT',
          o.batch,
          'BLOCK',
          Math.min(1000, o.claimIdleMs / 2),
          'STREAMS',
          o.stream,
          '>',
        )) as [string, [string, string[]][]][] | null;
        if (res?.[0]) await process(toEntries(res[0][1]));
      } catch (err) {
        if (!running) break;
        o.log.error({ err, stream: o.stream }, 'consumer loop error; backing off');
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  })();

  /** Stops reading new entries and resolves once the in-flight batch has finished. */
  return async () => {
    running = false;
    await done;
  };
}
