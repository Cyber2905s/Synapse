import pg from 'pg';
import type { Channel } from './catalog.ts';
import { defaultPreferences, type Preferences, type PreferencesInput } from './preferences.ts';

export type Db = pg.Pool;

export const DELIVERY_STATUSES = ['queued', 'sent', 'delivered', 'failed'] as const;
export type DeliveryStatus = (typeof DELIVERY_STATUSES)[number];

export const createDb = (connectionString: string, max = 10): Db =>
  new pg.Pool({ connectionString, max });

const SCHEMA = `
CREATE TABLE IF NOT EXISTS events (
  id          text PRIMARY KEY,
  type        text NOT NULL,
  data        jsonb NOT NULL,
  occurred_at timestamptz NOT NULL,
  received_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS preferences (
  user_id        text PRIMARY KEY,
  channels       jsonb NOT NULL,
  quiet_hours    jsonb,
  email          text,
  webhook_url    text,
  webhook_secret text,
  updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS deliveries (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id     text NOT NULL REFERENCES events(id),
  user_id      text NOT NULL,
  channel      text NOT NULL CHECK (channel IN ('in_app', 'email', 'webhook')),
  status       text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'sent', 'delivered', 'failed')),
  title        text NOT NULL,
  body         text NOT NULL,
  attempts     int  NOT NULL DEFAULT 0,
  last_error   text,
  -- Held by the worker currently attempting the send; guarantees one in-flight attempt per delivery.
  lease_until  timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now(),
  sent_at      timestamptz,
  delivered_at timestamptz,
  updated_at   timestamptz NOT NULL DEFAULT now(),
  -- Dedup by event id: one notification per (event, user, channel) no matter how often
  -- the event is published or redelivered.
  UNIQUE (event_id, user_id, channel)
);

CREATE INDEX IF NOT EXISTS deliveries_inbox_idx ON deliveries (user_id, created_at DESC) WHERE channel = 'in_app';
CREATE INDEX IF NOT EXISTS deliveries_status_idx ON deliveries (status);
`;

/** Idempotent; the advisory lock serialises concurrent API/worker boots. */
export async function migrate(db: Db): Promise<void> {
  const client = await db.connect();
  try {
    await client.query('SELECT pg_advisory_lock(7331)');
    await client.query(SCHEMA);
  } finally {
    await client.query('SELECT pg_advisory_unlock(7331)').catch(() => undefined);
    client.release();
  }
}

interface PreferencesRow {
  user_id: string;
  channels: Record<Channel, boolean>;
  quiet_hours: Preferences['quietHours'];
  email: string | null;
  webhook_url: string | null;
  webhook_secret: string | null;
}

const fromRow = (r: PreferencesRow): Preferences => ({
  userId: r.user_id,
  channels: r.channels,
  quietHours: r.quiet_hours,
  email: r.email,
  webhookUrl: r.webhook_url,
  webhookSecret: r.webhook_secret,
});

export async function getPreferences(db: Db, userId: string): Promise<Preferences> {
  const { rows } = await db.query<PreferencesRow>('SELECT * FROM preferences WHERE user_id = $1', [
    userId,
  ]);
  return rows[0] ? fromRow(rows[0]) : defaultPreferences(userId);
}

export async function getPreferencesMany(
  db: Db,
  userIds: string[],
): Promise<Map<string, Preferences>> {
  const { rows } = await db.query<PreferencesRow>(
    'SELECT * FROM preferences WHERE user_id = ANY($1)',
    [userIds],
  );
  const found = new Map(rows.map((r) => [r.user_id, fromRow(r)]));
  return new Map(userIds.map((id) => [id, found.get(id) ?? defaultPreferences(id)]));
}

export async function savePreferences(
  db: Db,
  userId: string,
  input: PreferencesInput,
): Promise<Preferences> {
  const current = await getPreferences(db, userId);
  const next: Preferences = {
    ...current,
    ...Object.fromEntries(Object.entries(input).filter(([, v]) => v !== undefined)),
    channels: { ...current.channels, ...input.channels },
    userId,
  };
  await db.query(
    `INSERT INTO preferences (user_id, channels, quiet_hours, email, webhook_url, webhook_secret)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (user_id) DO UPDATE SET channels = $2, quiet_hours = $3, email = $4,
       webhook_url = $5, webhook_secret = $6, updated_at = now()`,
    [userId, next.channels, next.quietHours, next.email, next.webhookUrl, next.webhookSecret],
  );
  return next;
}
