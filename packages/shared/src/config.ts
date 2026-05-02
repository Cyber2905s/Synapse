import { z } from 'zod';

const schema = z.object({
  NODE_ENV: z.string().default('development'),
  LOG_LEVEL: z.string().default('info'),
  PORT: z.coerce.number().int().default(3000),
  METRICS_PORT: z.coerce.number().int().default(9100),
  DATABASE_URL: z.string().default('postgres://synapse:synapse@localhost:5432/synapse'),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  SMTP_HOST: z.string().default('localhost'),
  SMTP_PORT: z.coerce.number().int().default(1025),
  MAIL_FROM: z.string().default('Synapse <notify@synapse.local>'),
  MAX_ATTEMPTS: z.coerce.number().int().min(1).default(5),
  BACKOFF_BASE_MS: z.coerce.number().int().min(1).default(500),
  BACKOFF_MAX_MS: z.coerce.number().int().min(1).default(60_000),
  RATE_LIMIT_PER_MINUTE: z.coerce.number().int().min(1).default(60),
  /** A pending stream entry idle this long is assumed orphaned by a dead worker and reclaimed. */
  CLAIM_IDLE_MS: z.coerce.number().int().min(100).default(30_000),
  WEBHOOK_TIMEOUT_MS: z.coerce.number().int().min(100).default(5_000),
});

export type Config = z.infer<typeof schema>;

export const loadConfig = (env: NodeJS.ProcessEnv = process.env): Config => schema.parse(env);
