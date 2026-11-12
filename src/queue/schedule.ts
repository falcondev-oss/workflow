import { Cron } from 'croner'

export function localTimeZone(): string {
  return new Intl.DateTimeFormat().resolvedOptions().timeZone
}

export function nextRunMs(pattern: string, tz: string, from: Date = new Date()): number | null {
  const next = new Cron(pattern, { timezone: tz }).nextRun(from)
  return next ? next.getTime() : null
}
