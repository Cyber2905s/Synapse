import { randomBytes } from 'node:crypto';
import { z } from 'zod';
import {
  getPreferences,
  preferencesInputSchema,
  savePreferences,
  type Preferences,
} from '@synapse/shared';
import type { Route } from '../app.ts';

export const userParams = z.object({ userId: z.string().min(1).max(128) });

const redact = (p: Preferences) => ({ ...p, webhookSecret: p.webhookSecret ? '********' : null });

// ponytail: no authn/authz — any caller can act as any user. Put a JWT check on /v1/users/:userId
// (and the WebSocket) before exposing this beyond a trusted network.
export const preferenceRoutes: Route = (app, { db }) => {
  app.get('/v1/users/:userId/preferences', async (req) => {
    const { userId } = userParams.parse(req.params);
    return redact(await getPreferences(db, userId));
  });

  app.put('/v1/users/:userId/preferences', async (req) => {
    const { userId } = userParams.parse(req.params);
    const input = preferencesInputSchema.parse(req.body);
    const current = await getPreferences(db, userId);
    // Setting a webhook without a secret: mint one and return it exactly once.
    const generated =
      input.webhookUrl && !input.webhookSecret && !current.webhookSecret
        ? randomBytes(24).toString('hex')
        : null;
    const saved = await savePreferences(
      db,
      userId,
      generated ? { ...input, webhookSecret: generated } : input,
    );
    return generated ? saved : redact(saved);
  });
};
