import { createHmac, timingSafeEqual } from 'node:crypto';

export const SIGNATURE_HEADER = 'x-synapse-signature';

/** Stripe-style signature: `t=<unix seconds>,v1=<hex hmac-sha256 of "t.body">`. */
export function signPayload(
  secret: string,
  body: string,
  timestamp = Math.floor(Date.now() / 1000),
): string {
  const mac = createHmac('sha256', secret).update(`${timestamp}.${body}`).digest('hex');
  return `t=${timestamp},v1=${mac}`;
}

/** Verifies a signature header; rejects stale timestamps to prevent replay. */
export function verifySignature(
  secret: string,
  body: string,
  header: string,
  toleranceSec = 300,
  now = Math.floor(Date.now() / 1000),
): boolean {
  const parts = Object.fromEntries(
    header.split(',').map((p) => p.split('=', 2) as [string, string]),
  );
  const t = Number(parts.t);
  if (!Number.isInteger(t) || !parts.v1 || Math.abs(now - t) > toleranceSec) return false;
  const expected = Buffer.from(signPayload(secret, body, t).split('v1=')[1] ?? '', 'hex');
  const actual = Buffer.from(parts.v1, 'hex');
  return actual.length === expected.length && timingSafeEqual(actual, expected);
}
