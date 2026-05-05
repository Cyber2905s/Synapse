import { z } from 'zod';

export const CHANNELS = ['in_app', 'email', 'webhook'] as const;
export type Channel = (typeof CHANNELS)[number];

export interface Template {
  title: string;
  body: string;
}

interface Route<S extends z.ZodType> {
  description: string;
  schema: S;
  /** Channels this event may be delivered on; user preferences can only narrow the list. */
  channels: Channel[];
  recipients: (data: z.infer<S>) => string[];
  template: Template;
  sample: z.infer<S>;
}

// Identity helper so each route's callbacks are typed against its own schema.
const route = <S extends z.ZodType>(r: Route<S>) =>
  r as unknown as Route<z.ZodType<Record<string, unknown>>>;

const userId = z.string().min(1).max(128);

/** The router: domain event type → payload schema, recipients, channels and template. */
export const catalog: Record<string, Route<z.ZodType<Record<string, unknown>>>> = {
  'order.shipped': route({
    description: 'An order left the warehouse',
    schema: z.object({ userId, orderId: z.string(), carrier: z.string(), trackingUrl: z.url() }),
    channels: ['in_app', 'email', 'webhook'],
    recipients: (d) => [d.userId],
    template: {
      title: 'Order {{orderId}} has shipped',
      body: 'Your order {{orderId}} is on its way with {{carrier}}. Track it at {{trackingUrl}}',
    },
    sample: {
      userId: 'alice',
      orderId: 'ORD-1042',
      carrier: 'DHL',
      trackingUrl: 'https://track.example.com/ORD-1042',
    },
  }),
  'order.delivered': route({
    description: 'An order was delivered',
    schema: z.object({ userId, orderId: z.string() }),
    channels: ['in_app', 'email'],
    recipients: (d) => [d.userId],
    template: {
      title: 'Order {{orderId}} delivered',
      body: 'Your order {{orderId}} was delivered. Enjoy!',
    },
    sample: { userId: 'alice', orderId: 'ORD-1042' },
  }),
  'comment.mentioned': route({
    description: 'One or more users were @mentioned in a comment',
    schema: z.object({
      userIds: z.array(userId).min(1).max(100),
      author: z.string(),
      snippet: z.string().max(280),
    }),
    channels: ['in_app', 'email'],
    recipients: (d) => [...new Set(d.userIds)],
    template: { title: '{{author}} mentioned you', body: '"{{snippet}}"' },
    sample: {
      userIds: ['alice', 'bob'],
      author: 'carol',
      snippet: '@alice @bob can you review the Q3 numbers?',
    },
  }),
  'payment.failed': route({
    description: 'A card charge was declined',
    schema: z.object({
      userId,
      amount: z.number().positive(),
      currency: z.string().length(3),
      reason: z.string(),
    }),
    channels: ['in_app', 'email', 'webhook'],
    recipients: (d) => [d.userId],
    template: {
      title: 'Payment of {{amount}} {{currency}} failed',
      body: 'We could not charge your card: {{reason}}. Please update your payment method.',
    },
    sample: { userId: 'alice', amount: 49.99, currency: 'USD', reason: 'insufficient funds' },
  }),
};

export const ingestEventSchema = z.object({
  /** Producer-assigned, globally unique. Re-publishing the same id is a no-op downstream. */
  id: z.string().min(1).max(200),
  type: z.string().min(1).max(100),
  data: z.record(z.string(), z.unknown()),
  occurredAt: z.iso.datetime().optional(),
});

export type IngestEvent = z.infer<typeof ingestEventSchema>;

/** Message placed on the events stream by the API. */
export interface StreamEvent extends IngestEvent {
  receivedAt: number;
}
