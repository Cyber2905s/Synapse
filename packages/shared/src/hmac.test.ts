import { describe, expect, it } from 'vitest';
import { signPayload, verifySignature } from './hmac.ts';

const body = JSON.stringify({ hello: 'world' });

describe('hmac', () => {
  it('round-trips', () => {
    expect(verifySignature('s3cret', body, signPayload('s3cret', body))).toBe(true);
  });

  it('rejects a wrong secret or tampered body', () => {
    const sig = signPayload('s3cret', body);
    expect(verifySignature('other', body, sig)).toBe(false);
    expect(verifySignature('s3cret', body + ' ', sig)).toBe(false);
  });

  it('rejects stale timestamps', () => {
    const sig = signPayload('s3cret', body, 1_000);
    expect(verifySignature('s3cret', body, sig, 300, 1_200)).toBe(true);
    expect(verifySignature('s3cret', body, sig, 300, 1_400)).toBe(false);
  });

  it('rejects malformed headers', () => {
    expect(verifySignature('s3cret', body, 'garbage')).toBe(false);
    expect(verifySignature('s3cret', body, 't=1,v1=zz')).toBe(false);
  });
});
