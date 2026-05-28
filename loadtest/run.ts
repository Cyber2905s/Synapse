/**
 * Load test: hammers POST /v1/events with autocannon, then waits for the pipeline to drain and
 * measures end-to-end latency (event received → in-app notification sent) from Postgres.
 *
 *   node loadtest/run.ts                      # defaults below
 *   TARGET=http://localhost:3000 DURATION=30 CONNECTIONS=200 node loadtest/run.ts
 */
import { randomUUID } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { cpus, loadavg, totalmem } from 'node:os';
import autocannon from 'autocannon';
import pg from 'pg';
import { GROUPS, KEYS, createRedis } from '@synapse/shared';

const TARGET = process.env.TARGET ?? 'http://localhost:3000';
const DURATION = Number(process.env.DURATION ?? 20);
const CONNECTIONS = Number(process.env.CONNECTIONS ?? 100);
const USERS = Number(process.env.USERS ?? 2000);
const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://synapse:synapse@localhost:5432/synapse';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';

const run = `lt${Date.now().toString(36)}`;
const loadAtStart = loadavg()[0]!;
const users = Array.from({ length: USERS }, (_, i) => `${run}-u${i}`);

// In-app only, so the numbers measure the pipeline rather than Mailpit's SMTP throughput.
console.log(`preparing ${USERS} users…`);
for (let i = 0; i < users.length; i += 100) {
  await Promise.all(
    users.slice(i, i + 100).map((u) =>
      fetch(`${TARGET}/v1/users/${u}/preferences`, {
        method: 'PUT',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ channels: { email: false, webhook: false } }),
      }),
    ),
  );
}

console.log(`ingest: ${CONNECTIONS} connections for ${DURATION}s against ${TARGET}`);
let n = 0;
const acked: string[] = []; // exact ids the API answered 202 for; each must end up delivered
const result = await autocannon({
  url: TARGET,
  connections: CONNECTIONS,
  duration: DURATION,
  requests: [
    {
      method: 'POST',
      path: '/v1/events',
      headers: { 'content-type': 'application/json' },
      setupRequest: (req) => ({
        ...req,
        body: JSON.stringify({
          id: randomUUID(),
          type: 'order.delivered',
          data: { userId: users[n++ % USERS], orderId: `ORD-${n}` },
        }),
      }),
      onResponse: (status, body) => {
        if (status === 202) acked.push((JSON.parse(body) as { id: string }).id);
      },
    },
  ],
});
const ingestEnded = Date.now();
const accepted = acked.length;
if (!accepted)
  throw new Error(`no events accepted (non-2xx: ${result.non2xx}, errors: ${result.errors})`);

const db = new pg.Pool({ connectionString: DATABASE_URL });
// Drained = both consumer groups have no lag and nothing pending, and no delivery is waiting in
// the delayed set. Polled from Redis so the measurement doesn't load Postgres.
const redis = createRedis(REDIS_URL);
const groupBacklog = async (stream: string, group: string) => {
  const groups = (await redis.xinfo('GROUPS', stream)) as (string | number | null)[][];
  for (const flat of groups) {
    const g = new Map<unknown, unknown>();
    for (let i = 0; i < flat.length; i += 2) g.set(flat[i], flat[i + 1]);
    if (g.get('name') === group) return Number(g.get('lag') ?? 0) + Number(g.get('pending') ?? 0);
  }
  return 0;
};
const backlog = async () =>
  (await groupBacklog(KEYS.events, GROUPS.router)) +
  (await groupBacklog(KEYS.deliveries, GROUPS.delivery)) +
  (await redis.zcard(KEYS.delayed));

console.log(`draining ${accepted} accepted events…`);
while ((await backlog()) > 0) await new Promise((r) => setTimeout(r, 250));
await redis.quit();
const drainMs = Date.now() - ingestEnded;

// Exact check, once: every acknowledged event id has a sent in-app notification.
const { rows: doneRows } = await db.query<{ done: number }>(
  `SELECT count(DISTINCT event_id)::int AS done FROM deliveries
   WHERE event_id = ANY($1) AND channel = 'in_app' AND status IN ('sent', 'delivered')`,
  [acked],
);
const done = doneRows[0]!.done;

const { rows } = await db.query<{
  p50: number;
  p95: number;
  p99: number;
  max: number;
  first: Date;
  last: Date;
}>(
  `SELECT
     percentile_cont(0.50) WITHIN GROUP (ORDER BY lat) AS p50,
     percentile_cont(0.95) WITHIN GROUP (ORDER BY lat) AS p95,
     percentile_cont(0.99) WITHIN GROUP (ORDER BY lat) AS p99,
     max(lat) AS max, min(first) AS first, max(last) AS last
   FROM (
     SELECT extract(epoch FROM d.sent_at - e.received_at) * 1000 AS lat, e.received_at AS first, d.sent_at AS last
     FROM deliveries d JOIN events e ON e.id = d.event_id
     WHERE d.event_id = ANY($1) AND d.sent_at IS NOT NULL
   ) t`,
  [acked],
);
const { rows: dupes } = await db.query<{ n: number }>(
  `SELECT count(*)::int AS n FROM (
     SELECT event_id FROM deliveries WHERE event_id = ANY($1) AND channel = 'in_app'
     GROUP BY event_id HAVING count(*) > 1) t`,
  [acked],
);
await db.end();
const e2e = rows[0]!;
const e2eSeconds = (e2e.last.getTime() - e2e.first.getTime()) / 1000;

const report = {
  run,
  at: new Date().toISOString(),
  machine: {
    cpus: cpus().length,
    cpuModel: cpus()[0]?.model,
    memGb: Math.round(totalmem() / 2 ** 30),
    loadAvg1mAtStart: Number(loadAtStart.toFixed(1)),
  },
  config: { target: TARGET, durationSec: DURATION, connections: CONNECTIONS, users: USERS },
  ingest: {
    requests: result.requests.total,
    accepted,
    non2xx: result.non2xx,
    errors: result.errors,
    timeouts: result.timeouts,
    rpsAvg: result.requests.average,
    latencyMs: {
      p50: result.latency.p50,
      p90: result.latency.p90,
      p99: result.latency.p99,
      max: result.latency.max,
    },
  },
  pipeline: {
    delivered: done,
    lost: accepted - done,
    duplicated: dupes[0]!.n,
    drainAfterIngestMs: drainMs,
    throughputPerSec: Math.round(done / e2eSeconds),
    e2eLatencyMs: {
      p50: Math.round(e2e.p50),
      p95: Math.round(e2e.p95),
      p99: Math.round(e2e.p99),
      max: Math.round(e2e.max),
    },
  },
};

mkdirSync(new URL('./results/', import.meta.url), { recursive: true });
writeFileSync(new URL(`./results/${run}.json`, import.meta.url), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
