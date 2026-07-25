import { Cron } from 'croner'

/** The queuer's local IANA zone, captured at registration when no explicit `tz` is given (§8). */
export function localTimeZone(): string {
  return Intl.DateTimeFormat().resolvedOptions().timeZone
}

/**
 * Next fire time (epoch ms) for a cron `pattern` in `tz`, strictly after `from` (default now);
 * `null` if the pattern has no future occurrence. Croner is the single cron authority (DST-safe,
 * native IANA tz). Passing `now` collapses any backlog to a single fire (missed-run = skip, §8).
 */
export function nextRunMs(pattern: string, tz: string, from: Date = new Date()): number | null {
  const next = new Cron(pattern, { timezone: tz }).nextRun(from)
  return next ? next.getTime() : null
}
