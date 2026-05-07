import type { Route } from '../app.ts';

export const healthRoutes: Route = (app, { db, redis, state }) => {
  /** Liveness: the process is up and the event loop responds. */
  app.get('/health', async () => ({ status: 'ok' }));

  /** Readiness: dependencies reachable and not draining. */
  app.get('/ready', async (_req, reply) => {
    const checks = await Promise.allSettled([db.query('SELECT 1'), redis.ping()]);
    const [pg, rd] = checks.map((c) => (c.status === 'fulfilled' ? 'ok' : 'down'));
    const ready = !state.shuttingDown && pg === 'ok' && rd === 'ok';
    return reply
      .code(ready ? 200 : 503)
      .send({ status: ready ? 'ready' : 'unavailable', postgres: pg, redis: rd });
  });
};
