import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { GROUPS, KEYS, type Db, type Redis } from '@synapse/shared';
import {
  api,
  connectInbox,
  countDeliveries,
  orderDelivered,
  resetInfra,
  sleep,
  startApi,
  startTestWorker,
  testEnv,
  uid,
  waitFor,
} from './helpers.ts';

const WORKER_ENTRY = fileURLToPath(new URL('../../apps/worker/src/index.ts', import.meta.url));
const CLAIM_IDLE_MS = 1500;

let db: Db;
let redis: Redis;
let apiServer: Awaited<ReturnType<typeof startApi>>;

beforeAll(async () => {
  ({ db, redis } = await resetInfra());
  apiServer = await startApi(db);
});

afterAll(async () => {
  await apiServer.close();
  await redis.quit();
  await db.end();
});

/** Runs the real worker binary as a separate OS process so it can be killed for real. */
async function spawnWorker(name: string, port: number): Promise<ChildProcess> {
  const child = spawn(process.execPath, [WORKER_ENTRY], {
    env: testEnv({
      WORKER_NAME: name,
      METRICS_PORT: String(port),
      CLAIM_IDLE_MS: String(CLAIM_IDLE_MS),
    }),
    stdio: 'ignore',
  });
  await waitFor(
    async () => (await fetch(`http://127.0.0.1:${port}/health`).catch(() => null))?.ok,
    { message: `${name} to boot` },
  );
  return child;
}

const pendingCount = async () => {
  const [ev, dl] = await Promise.all([
    redis.xpending(KEYS.events, GROUPS.router),
    redis.xpending(KEYS.deliveries, GROUPS.delivery),
  ]);
  return Number((ev as [number])[0]) + Number((dl as [number])[0]);
};

/** Publishes events one by one in the background so the worker is busy when it gets killed. */
function publishStream(userId: string, n: number) {
  const events = Array.from({ length: n }, () => orderDelivered(userId));
  const done = (async () => {
    for (const e of events) expect((await api(apiServer.url, '/v1/events', e)).status).toBe(202);
  })();
  return { events, done };
}

async function assertExactlyOnce(
  userId: string,
  inbox: Awaited<ReturnType<typeof connectInbox>>,
  eventIds: string[],
) {
  const n = eventIds.length;
  await waitFor(
    async () => (await countDeliveries(db, `user_id = $1 AND status = 'sent'`, [userId])) === n,
    { timeout: 30_000, message: `all ${n} deliveries sent` },
  );
  await waitFor(() => inbox.received.length >= n, { message: 'all pushes received' });
  // Give any would-be duplicate (reclaim + redelivery) time to surface before asserting.
  await sleep(CLAIM_IDLE_MS * 2);

  const ids = inbox.received.map((r) => r.id);
  expect(ids).toHaveLength(n); // nothing pushed twice
  expect(new Set(ids).size).toBe(n);
  expect(new Set(inbox.received.map((r) => r.eventId))).toEqual(new Set(eventIds)); // nothing lost
  expect(await countDeliveries(db, 'user_id = $1', [userId])).toBe(n);
  const inboxApi = await api<unknown[]>(
    apiServer.url,
    `/v1/users/${userId}/notifications?limit=500`,
  );
  expect(inboxApi.body).toHaveLength(n);
  expect(await pendingCount()).toBe(0);
}

describe('worker crash safety', () => {
  it('SIGKILL mid-delivery: no notification is lost or duplicated', async () => {
    const user = uid('crash');
    await api(
      apiServer.url,
      `/v1/users/${user}/preferences`,
      { channels: { email: false } },
      'PUT',
    );
    const inbox = await connectInbox(apiServer.url, user);
    const victim = await spawnWorker('victim', 9291);

    const N = 400;
    const { events, done } = publishStream(user, N);
    await waitFor(
      async () => (await countDeliveries(db, `user_id = $1 AND status = 'sent'`, [user])) >= N / 4,
      { interval: 5, message: 'victim to make progress' },
    );

    victim.kill('SIGKILL');
    await once(victim, 'exit');
    const sentAtKill = await countDeliveries(db, `user_id = $1 AND status = 'sent'`, [user]);
    const inFlightAtKill = await pendingCount();
    // Prove the kill really landed mid-stream: some work done, some still unacknowledged.
    expect(sentAtKill).toBeGreaterThan(0);
    expect(sentAtKill).toBeLessThan(N);
    expect(inFlightAtKill).toBeGreaterThan(0);

    await done;
    const stopSurvivor = await startTestWorker(db, undefined, 'survivor');
    try {
      await assertExactlyOnce(
        user,
        inbox,
        events.map((e) => e.id),
      );
    } finally {
      await stopSurvivor();
      inbox.ws.close();
    }
  });

  it('SIGTERM drains gracefully and exits 0 without losing work', async () => {
    const user = uid('drain');
    await api(
      apiServer.url,
      `/v1/users/${user}/preferences`,
      { channels: { email: false } },
      'PUT',
    );
    const inbox = await connectInbox(apiServer.url, user);
    const worker = await spawnWorker('drainer', 9292);

    const { events, done } = publishStream(user, 200);
    await waitFor(async () => (await countDeliveries(db, 'user_id = $1', [user])) >= 20, {
      interval: 5,
    });
    worker.kill('SIGTERM');
    const [code] = (await once(worker, 'exit')) as [number];
    expect(code).toBe(0);

    await done;
    const stopSurvivor = await startTestWorker(db, undefined, 'survivor-2');
    try {
      await assertExactlyOnce(
        user,
        inbox,
        events.map((e) => e.id),
      );
    } finally {
      await stopSurvivor();
      inbox.ws.close();
    }
  });
});
