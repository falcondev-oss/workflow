export interface ExpBackoffOptions {
  /** First-retry delay in ms. Default: 500. */
  base?: number
  /** Per-attempt multiplier. Default: 2. */
  factor?: number
  /** Upper bound (ms) on the un-jittered delay. Default: 30_000. */
  cap?: number
}

/**
 * Default retry backoff: exponential with full jitter (§10). `attempt` is 1-based — the
 * number of the attempt that just failed. Returns a uniform random delay in
 * `[0, min(cap, base * factor^(attempt-1))]` ms, so retries spread out instead of
 * thundering. Pure; the worker evaluates it at fail time and stores the resulting `runAt`.
 */
export function expBackoff(opts?: ExpBackoffOptions): (attempt: number) => number {
  const base = opts?.base ?? 500
  const factor = opts?.factor ?? 2
  const cap = opts?.cap ?? 30_000
  return (attempt) => Math.random() * Math.min(cap, base * factor ** Math.max(0, attempt - 1))
}
