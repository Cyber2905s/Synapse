import { z } from 'zod';
import { quietHoursSchema, type QuietHours } from './quiet-hours.ts';
import type { Channel } from './catalog.ts';

export const preferencesInputSchema = z
  .object({
    channels: z.object({ in_app: z.boolean(), email: z.boolean(), webhook: z.boolean() }).partial(),
    quietHours: quietHoursSchema.nullable(),
    email: z.email().nullable(),
    webhookUrl: z.url({ protocol: /^https?$/ }).nullable(),
    webhookSecret: z.string().min(16).max(256).nullable(),
  })
  .partial();

export type PreferencesInput = z.infer<typeof preferencesInputSchema>;

export interface Preferences {
  userId: string;
  channels: Record<Channel, boolean>;
  quietHours: QuietHours | null;
  email: string | null;
  webhookUrl: string | null;
  webhookSecret: string | null;
}

export const defaultPreferences = (userId: string): Preferences => ({
  userId,
  channels: { in_app: true, email: true, webhook: false },
  quietHours: null,
  email: `${userId}@example.com`,
  webhookUrl: null,
  webhookSecret: null,
});
