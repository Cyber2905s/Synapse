import { KEYS, catalog, ingestEventSchema, type StreamEvent } from '@synapse/shared';
import type { Route } from '../app.ts';
import { eventsIngested } from '../metrics.ts';

export const eventRoutes: Route = (app, { redis }) => {
  app.get('/v1/event-types', async () =>
    Object.entries(catalog).map(([type, r]) => ({
      type,
      description: r.description,
      channels: r.channels,
      template: r.template,
      sample: r.sample,
    })),
  );

  /**
   * Accepts a domain event and appends it to the events stream. Deliberately does no database work
   * on the hot path: dedup by event id happens downstream, so producers can safely retry.
   */
  app.post('/v1/events', async (req, reply) => {
    const event = ingestEventSchema.parse(req.body);
    const route = catalog[event.type];
    if (!route) return reply.code(422).send({ error: `unknown event type: ${event.type}` });
    const data = route.schema.parse(event.data);

    const message: StreamEvent = { ...event, data, receivedAt: Date.now() };
    const streamId = await redis.xadd(KEYS.events, '*', 'payload', JSON.stringify(message));
    eventsIngested.inc({ type: event.type });
    return reply.code(202).send({ id: event.id, streamId });
  });
};
