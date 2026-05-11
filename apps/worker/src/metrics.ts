import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';
import { KEYS, type Redis } from '@synapse/shared';

export const registry = new Registry();
collectDefaultMetrics({ register: registry });

export const eventsRouted = new Counter({
  name: 'synapse_events_routed_total',
  help: 'Events fanned out into deliveries by the router stage',
  labelNames: ['type'],
  registers: [registry],
});

export const deliveries = new Counter({
  name: 'synapse_deliveries_total',
  help: 'Delivery attempt outcomes',
  labelNames: ['channel', 'outcome'], // sent | delivered | retry | dead | deferred
  registers: [registry],
});

export const deliveryRetries = new Counter({
  name: 'synapse_delivery_retries_total',
  help: 'Failed attempts scheduled for retry with backoff',
  labelNames: ['channel'],
  registers: [registry],
});

export const deliveryLatency = new Histogram({
  name: 'synapse_delivery_latency_seconds',
  help: 'Time from event ingestion to successful hand-off on the channel',
  labelNames: ['channel'],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15, 60, 300],
  registers: [registry],
});

export function registerDlqGauge(redis: Redis) {
  new Gauge({
    name: 'synapse_dlq_size',
    help: 'Entries currently in the dead-letter queue',
    registers: [registry],
    async collect() {
      this.set(await redis.xlen(KEYS.dlq));
    },
  });
}
