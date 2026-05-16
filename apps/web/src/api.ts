export type Channel = 'in_app' | 'email' | 'webhook';
export type Status = 'queued' | 'sent' | 'delivered' | 'failed';

export interface Notification {
  id: string;
  eventId: string;
  userId: string;
  channel: Channel;
  status: Status;
  title: string;
  body: string;
  createdAt: string;
  attempts?: number;
  lastError?: string | null;
}

export interface EventType {
  type: string;
  description: string;
  channels: Channel[];
  sample: Record<string, unknown>;
}

export interface Preferences {
  channels: Record<Channel, boolean>;
  quietHours: { start: string; end: string; timezone: string } | null;
  email: string | null;
  webhookUrl: string | null;
}

export interface DlqEntry {
  entryId: string;
  deliveryId: string;
  error: string;
  failedAt: number;
  delivery: { userId: string; channel: Channel; title: string; attempts: number } | null;
}

export interface Sink {
  url: string;
  failing: boolean;
  received: { deliveryId: string; verified: boolean; receivedAt: number }[];
}

export async function get<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${path} responded ${res.status}`);
  return res.json() as Promise<T>;
}

export async function send<T>(path: string, body: unknown, method = 'POST') {
  const res = await fetch(path, {
    method,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: (await res.json().catch(() => null)) as T };
}

export const MAILPIT_URL = import.meta.env.VITE_MAILPIT_URL ?? 'http://localhost:8025';
