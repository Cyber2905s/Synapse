import { pino } from 'pino';

export const createLogger = (name: string, level = process.env.LOG_LEVEL ?? 'info') =>
  pino({ name, level });

export type Logger = ReturnType<typeof createLogger>;
