import { createServer } from 'node:http';
import { hostname } from 'node:os';
import { createDb, createLogger, createRedis, loadConfig, migrate } from '@synapse/shared';
import { registerDlqGauge, registry } from './metrics.ts';
import { startWorker } from './worker.ts';

const config = loadConfig();
const log = createLogger('worker', config.LOG_LEVEL);
const db = createDb(config.DATABASE_URL);
const redis = createRedis(config.REDIS_URL);
let shuttingDown = false;

await migrate(db);
registerDlqGauge(redis);
const stop = await startWorker({
  config,
  db,
  redis,
  log,
  name: process.env.WORKER_NAME ?? `${hostname()}-${process.pid}`,
});

// Metrics + health on a side port; workers have no other HTTP surface.
const server = createServer(async (req, res) => {
  if (req.url === '/metrics') {
    res.writeHead(200, { 'content-type': registry.contentType }).end(await registry.metrics());
  } else if (req.url === '/health') {
    res.writeHead(shuttingDown ? 503 : 200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ status: shuttingDown ? 'draining' : 'ok' }));
  } else {
    res.writeHead(404).end();
  }
}).listen(config.METRICS_PORT);

async function shutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info({ signal }, 'draining in-flight deliveries');
  const force = setTimeout(() => process.exit(1), 25_000).unref();
  try {
    await stop(); // finishes the current batch; anything unacked is reclaimed by peers
    server.close();
    await Promise.all([redis.quit(), db.end()]);
    clearTimeout(force);
    log.info('shutdown complete');
    process.exit(0);
  } catch (err) {
    log.error({ err }, 'shutdown failed');
    process.exit(1);
  }
}
process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));
