import type { Route } from '../app.ts';
import { httpDuration, registry } from '../metrics.ts';

export const metricsRoutes: Route = (app) => {
  app.addHook('onResponse', async (req, reply) => {
    httpDuration.observe(
      { method: req.method, route: req.routeOptions.url ?? 'unmatched', status: reply.statusCode },
      reply.elapsedTime / 1000,
    );
  });

  app.get('/metrics', async (_req, reply) =>
    reply.type(registry.contentType).send(await registry.metrics()),
  );
};
