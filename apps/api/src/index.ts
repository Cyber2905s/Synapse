import { createDb, createRedis, loadConfig, migrate } from '@synapse/shared';
import { buildApp } from './app.ts';

const config = loadConfig();
const db = createDb(config.DATABASE_URL);
const redis = createRedis(config.REDIS_URL);
const sub = createRedis(config.REDIS_URL);
const state = { shuttingDown: false };

await migrate(db);
const app = await buildApp({ config, db, redis, sub, state });
await app.listen({ port: config.PORT, host: '0.0.0.0' });

async function shutdown(signal: string) {
  if (state.shuttingDown) return;
  state.shuttingDown = true;
  app.log.info({ signal }, 'shutting down');
  const force = setTimeout(() => process.exit(1), 10_000).unref();
  try {
    await app.close(); // stops accepting, drains in-flight requests, closes WebSockets
    await Promise.all([sub.quit(), redis.quit(), db.end()]);
    clearTimeout(force);
    process.exit(0);
  } catch (err) {
    app.log.error({ err }, 'shutdown failed');
    process.exit(1);
  }
}
process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));
