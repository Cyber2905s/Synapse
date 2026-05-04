import { describe, expect, it } from 'vitest';
import { msUntilQuietEnds, quietHoursSchema } from './quiet-hours.ts';

const at = (iso: string) => new Date(iso);
const min = 60_000;

describe('msUntilQuietEnds', () => {
  const wrap = { start: '22:00', end: '07:00', timezone: 'UTC' };

  it('handles windows that wrap midnight', () => {
    expect(msUntilQuietEnds(wrap, at('2026-01-01T23:00:00Z'))).toBe(8 * 60 * min);
    expect(msUntilQuietEnds(wrap, at('2026-01-01T06:30:00Z'))).toBe(30 * min);
    expect(msUntilQuietEnds(wrap, at('2026-01-01T07:00:00Z'))).toBe(0);
    expect(msUntilQuietEnds(wrap, at('2026-01-01T12:00:00Z'))).toBe(0);
  });

  it('handles same-day windows', () => {
    const day = { start: '09:00', end: '17:00', timezone: 'UTC' };
    expect(msUntilQuietEnds(day, at('2026-01-01T16:59:30Z'))).toBe(30_000);
    expect(msUntilQuietEnds(day, at('2026-01-01T08:59:00Z'))).toBe(0);
  });

  it('evaluates in the user timezone', () => {
    // 20:00 UTC is 01:30 in Kolkata (+05:30), inside 22:00-07:00.
    const ist = { ...wrap, timezone: 'Asia/Kolkata' };
    expect(msUntilQuietEnds(ist, at('2026-01-01T20:00:00Z'))).toBe((5 * 60 + 30) * min);
  });

  it('validates input', () => {
    expect(
      quietHoursSchema.safeParse({ start: '25:00', end: '07:00', timezone: 'UTC' }).success,
    ).toBe(false);
    expect(
      quietHoursSchema.safeParse({ start: '22:00', end: '07:00', timezone: 'Mars/Base' }).success,
    ).toBe(false);
  });
});
