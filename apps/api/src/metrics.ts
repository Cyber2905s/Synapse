import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

export const registry = new Registry();
collectDefaultMetrics({ register: registry });

export const eventsIngested = new Counter({
  name: 'synapse_events_ingested_total',
  help: 'Domain events accepted by the ingestion endpoint (rate() gives events/sec)',
  labelNames: ['type'],
  registers: [registry],
});

export const wsConnections = new Gauge({
  name: 'synapse_ws_connections',
  help: 'Open WebSocket connections on this instance',
  registers: [registry],
});

export const httpDuration = new Histogram({
  name: 'synapse_http_request_duration_seconds',
  help: 'HTTP request latency',
  labelNames: ['method', 'route', 'status'],
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
  registers: [registry],
});
