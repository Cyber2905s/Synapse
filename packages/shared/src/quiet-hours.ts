import { z } from 'zod';

const hhmm = z.string().regex(/^([01]\d|2[0-3]):[0-5]\d$/, 'expected HH:MM');

export const quietHoursSchema = z.object({
  start: hhmm,
  end: hhmm,
  timezone: z.string().refine((tz) => {
    try {
      new Intl.DateTimeFormat('en', { timeZone: tz });
      return true;
    } catch {
      return false;
    }
  }, 'unknown IANA timezone'),
});

export type QuietHours = z.infer<typeof quietHoursSchema>;

const toMinutes = (s: string) => Number(s.slice(0, 2)) * 60 + Number(s.slice(3, 5));

/**
 * Milliseconds until the user's quiet window ends, or 0 if `now` is outside it.
 * Windows may wrap midnight (22:00 → 07:00).
 * ponytail: minute granularity and DST transitions inside a window are ignored (off by up to 1h
 * on those two nights); compute with a tz library if that ever matters.
 */
export function msUntilQuietEnds(qh: QuietHours, now: Date): number {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: qh.timezone,
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(now);
  const get = (type: string) => Number(parts.find((p) => p.type === type)?.value);
  const m = get('hour') * 60 + get('minute');
  const s = toMinutes(qh.start);
  const e = toMinutes(qh.end);
  const inside = s <= e ? m >= s && m < e : m >= s || m < e;
  if (!inside) return 0;
  const minutesLeft = (e - m + 1440) % 1440;
  return minutesLeft * 60_000 - now.getUTCSeconds() * 1000 - now.getUTCMilliseconds();
}
