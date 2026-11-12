export interface ExpBackoffOptions {
  base?: number
  factor?: number
  cap?: number
}

export function expBackoff(opts?: ExpBackoffOptions): (attempt: number) => number {
  const base = opts?.base ?? 500
  const factor = opts?.factor ?? 2
  const cap = opts?.cap ?? 30_000
  return (attempt) => Math.random() * Math.min(cap, base * factor ** Math.max(0, attempt - 1))
}
