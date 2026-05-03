/**
 * Exponential backoff with "equal jitter": half the exponential delay is fixed, half is random.
 * Keeps a floor (so retries never hammer a failing target) while still de-synchronising workers.
 * `attempt` is 1-based: the delay before retry #attempt.
 */
export function backoffDelay(
  attempt: number,
  baseMs: number,
  maxMs: number,
  random = Math.random,
): number {
  const exp = Math.min(maxMs, baseMs * 2 ** (attempt - 1));
  return Math.round(exp / 2 + random() * (exp / 2));
}
