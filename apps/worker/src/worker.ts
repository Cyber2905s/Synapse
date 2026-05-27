import nodemailer from 'nodemailer';
import {
  GROUPS,
  KEYS,
  ensureGroup,
  type Config,
  type Db,
  type Logger,
  type Redis,
  type StreamEvent,
} from '@synapse/shared';
import { startConsumer } from './consumer.ts';
import type { WorkerContext } from './context.ts';
import { deliverBatch } from './deliver.ts';
import { routeEvents } from './router.ts';

/** Atomically moves due delivery ids from the delayed set back onto the deliveries stream. */
const PROMOTE = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 500)
for _, id in ipairs(due) do
  redis.call('ZREM', KEYS[1], id)
  redis.call('XADD', KEYS[2], '*', 'deliveryId', id)
end
return #due`;

export interface WorkerDeps {
  config: Config;
  db: Db;
  redis: Redis;
  log: Logger;
  /** Consumer name within the groups; must be unique per live worker. */
  name: string;
}

export async function startWorker({ config, db, redis, log, name }: WorkerDeps) {
  await ensureGroup(redis, KEYS.events, GROUPS.router);
  await ensureGroup(redis, KEYS.deliveries, GROUPS.delivery);

  const mailer = nodemailer.createTransport({
    host: config.SMTP_HOST,
    port: config.SMTP_PORT,
    secure: false,
    pool: true,
  });
  const ctx: WorkerContext = { config, db, redis, log, mailer };
  const blocking = [redis.duplicate(), redis.duplicate()] as const;
  const common = { consumer: name, claimIdleMs: config.CLAIM_IDLE_MS, batch: 50, log };

  const stops = [
    startConsumer({
      ...common,
      redis: blocking[0],
      stream: KEYS.events,
      group: GROUPS.router,
      handle: (batch) =>
        routeEvents(
          ctx,
          batch.map((e) => JSON.parse(e.fields.payload!) as StreamEvent),
        ),
    }),
    startConsumer({
      ...common,
      redis: blocking[1],
      stream: KEYS.deliveries,
      group: GROUPS.delivery,
      handle: (batch) =>
        deliverBatch(
          ctx,
          batch.map((e) => e.fields.deliveryId!),
        ),
    }),
  ];

  let promoting: Promise<unknown> = Promise.resolve();
  const promoter = setInterval(() => {
    promoting = redis
      .eval(PROMOTE, 2, KEYS.delayed, KEYS.deliveries, Date.now())
      .catch((err: unknown) => log.error({ err }, 'delayed promotion failed'));
  }, 250);

  log.info({ name }, 'worker started');

  return async function stop() {
    clearInterval(promoter);
    await Promise.all(stops.map((s) => s()));
    await promoting;
    mailer.close();
    // disconnect() rather than quit(): a blocked XREADGROUP would otherwise hold quit() open.
    for (const c of blocking) c.disconnect();
  };
}
