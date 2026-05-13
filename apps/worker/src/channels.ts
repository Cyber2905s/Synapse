import { SIGNATURE_HEADER, signPayload, type Preferences } from '@synapse/shared';
import type { WorkerContext } from './context.ts';
import type { DeliveryRow } from './deliver.ts';

export async function sendEmail(
  ctx: WorkerContext,
  d: DeliveryRow,
  prefs: Preferences,
): Promise<void> {
  if (!prefs.email) throw new Error('no email address on file');
  await ctx.mailer.sendMail({
    from: ctx.config.MAIL_FROM,
    to: prefs.email,
    subject: d.title,
    text: d.body,
    // Stable Message-ID per delivery: if a crash forces a resend, mail clients can collapse it.
    messageId: `<${d.id}@synapse.local>`,
  });
}

export async function sendWebhook(
  ctx: WorkerContext,
  d: DeliveryRow,
  prefs: Preferences,
): Promise<void> {
  if (!prefs.webhookUrl || !prefs.webhookSecret) throw new Error('webhook not configured');
  const body = JSON.stringify({
    deliveryId: d.id,
    eventId: d.event_id,
    type: d.event_type,
    userId: d.user_id,
    title: d.title,
    body: d.body,
    data: d.event_data,
  });
  const res = await fetch(prefs.webhookUrl, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      [SIGNATURE_HEADER]: signPayload(prefs.webhookSecret, body),
      // Idempotency key for receivers: at-least-once means they may see a delivery twice.
      'x-synapse-delivery': d.id,
    },
    body,
    signal: AbortSignal.timeout(ctx.config.WEBHOOK_TIMEOUT_MS),
    redirect: 'error',
  });
  if (!res.ok) throw new Error(`webhook responded ${res.status}`);
}
