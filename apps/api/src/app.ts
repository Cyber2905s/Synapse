import Fastify, { type FastifyInstance } from 'fastify';
import { z } from 'zod';
import type { Config, Db, Redis } from '@synapse/shared';
import { healthRoutes } from './routes/health.ts';
import { metricsRoutes } from './routes/metrics.ts';
import { eventRoutes } from './routes/events.ts';

export interface Deps {
  config: Config;
  db: Db;
  /** General-purpose connection (commands, XADD, PUBLISH). */
  redis: Redis;
  /** Dedicated subscriber connection for WebSocket fan-out. */
  sub: Redis;
  /** Flipped on SIGTERM so readiness fails and load balancers drain us first. */
  state: { shuttingDown: boolean };
}

export type Route = (app: FastifyInstance, deps: Deps) => void;

export async function buildApp(deps: Deps): Promise<FastifyInstance> {
  const app = Fastify({
    logger: { name: 'api', level: deps.config.LOG_LEVEL },
    bodyLimit: 256 * 1024,
    disableRequestLogging: deps.config.NODE_ENV === 'production',
  });

  app.setErrorHandler((err, req, reply) => {
    if (err instanceof z.ZodError) {
      return reply.code(400).send({ error: 'validation_failed', issues: err.issues });
    }
    const status = (err as { statusCode?: number }).statusCode ?? 500;
    if (status >= 500) req.log.error({ err }, 'request failed');
    return reply
      .code(status)
      .send({ error: status >= 500 ? 'internal_error' : (err as Error).message });
  });

  for (const route of [healthRoutes, metricsRoutes, eventRoutes]) route(app, deps);
  return app;
}
