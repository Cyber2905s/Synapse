import type { Transporter } from 'nodemailer';
import type { Config, Db, Logger, Redis } from '@synapse/shared';

export interface WorkerContext {
  config: Config;
  db: Db;
  /** Command connection (non-blocking calls only). */
  redis: Redis;
  log: Logger;
  mailer: Transporter;
}
