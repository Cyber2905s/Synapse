import { describe, expect, it } from 'vitest';
import { backoffDelay } from './backoff.ts';

describe('backoffDelay', () => {
  it('doubles the window each attempt', () => {
    expect([1, 2, 3, 4].map((a) => backoffDelay(a, 100, 10_000, () => 1))).toEqual([
      100, 200, 400, 800,
    ]);
  });

  it('keeps half the window as a floor', () => {
    expect([1, 2, 3].map((a) => backoffDelay(a, 100, 10_000, () => 0))).toEqual([50, 100, 200]);
  });

  it('caps at maxMs', () => {
    expect(backoffDelay(30, 100, 5_000, () => 1)).toBe(5_000);
  });

  it('stays within [exp/2, exp] with real randomness', () => {
    for (let i = 0; i < 1000; i++) {
      const d = backoffDelay(3, 100, 10_000);
      expect(d).toBeGreaterThanOrEqual(200);
      expect(d).toBeLessThanOrEqual(400);
    }
  });
});
