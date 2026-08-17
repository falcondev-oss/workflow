/**
 * PROTOTYPE — throwaway, not production. Answers issue #16 "The Driver interface in TypeScript".
 *
 * The seam every backend implements. Cut at the *op* level: `Queue`, `Worker`, the wake loop, the
 * cron tick, the retry policy, the public value types and the error classes stay shared and written
 * once; the driver is only the storage ops. A descriptor (`redisDriver({…})`) returns a
 * {@link DriverFactory} that `Namespace` calls once with its own identity, producing a
 * namespace-scoped instance holding the connections and the prefix/schema.
 *
 * Conventions, everywhere and without exception:
 * - **Named object params.** No positional args — the Lua `ARGV` tuple was `defineCommand`'s
 *   constraint, not a contract.
 * - **Milliseconds, always.** No second-valued params at the seam even where the public option is
 *   in seconds (`resultTtl`) or the Redis command is (`BRPOP`, `EXPIRE`).
 * - **`number | null`**, never `-1`/`0` sentinels.
 * - **Flat string-literal unions for expected outcomes**; a thrown error means the backend genuinely
 *   failed. Nothing at this seam is signalled by an error message's text, and no outcome echoes
 *   back an argument.
 * - **Named after intent, never after a Redis access pattern** (#14's rule: `getScheduleIds` +
 *   `getSchedule` would have exported Redis' N+1).
 * - **Rows out, not domain values.** The driver returns the flattest thing that carries the data;
 *   `ReservedJob` and every other public type is minted by the shared layer.
 */

import type { QueueMetrics, ScheduleInfo, WorkflowLogger } from './types'

/* ────────────────────────────── construction & lifecycle ────────────────────────────── */

/**
 * What `redisDriver({…})` / `postgresDriver({…})` return. `Namespace` calls it once, in its
 * constructor, with the identity the whole instance is scoped to: the ns id, the ns concurrency
 * ceiling and the logger are fixed at construction and never vary per call, so they are handed over
 * once. Every ceiling that *does* vary — workflow, group, lock — stays per-call, because those are
 * per-queue and per-worker.
 *
 * `logger` is here rather than in the shared layer alone because #13's schema-skew path has to warn
 * from inside the driver.
 */
export type DriverFactory = (ctx: {
  /** Namespace id — scopes the cross-workflow concurrency cap and the wake channel. */
  nsId: string
  /** Namespace concurrency ceiling across all its workflows. `null` = unlimited. */
  concurrency: number | null
  logger?: WorkflowLogger
}) => WorkflowDriver

export type WorkflowDriver = {
  /**
   * Ready to serve ops — *not* "run migrations" wearing a general name (#13). Redis connects or
   * does nothing depending on the descriptor variant; Postgres runs the advisory-locked migration
   * gate. Called once by `Namespace`, which memoises it lazily and **clears the memo on
   * rejection** so the caller's next op is the retry.
   */
  init: () => Promise<void>

  /**
   * Releases what the driver created and nothing it was handed (#11). **Idempotent** — an explicit
   * `close()` and the `exit-hook` can both fire. `Namespace.close()` cascades here only after
   * draining its queues and workers (`Worker.close()` already awaits `inProgress`), so no op is
   * ever in flight across it and the driver never has to abort one.
   */
  close: () => Promise<void>

  /**
   * Max jobs one {@link reserve} may claim. A backend property, not a tuning knob: Redis' 64 exists
   * because Lua blocks the single thread; Postgres measured flat to 256 (#12). Lives on the
   * interface because `Worker` compares `claims.length === want` to decide "the cap, not an empty
   * queue, ended this pass" — a driver that silently clamped `want` would turn that into a park,
   * i.e. a silent throughput cliff.
   */
  readonly maxReserveBatch: number

  /* ──────────────────────────────────── atomic ops ──────────────────────────────────── */

  /**
   * `runAt` is absolute epoch-ms, resolved by the shared layer: `runIn` becomes `Date.now() +
   * runIn` before it gets here, so the seam has one delay param rather than Redis' `(runAt, runIn)`
   * pair of `-1`s. That moves the origin from `redis.call("TIME")` to the client clock, which
   * `fail`'s backoff has always used, against #12's measured 1.6ms drift.
   *
   * `'duplicate'` replaces the `err.message.includes('JobAlreadyExists')` match — the shared layer
   * throws `JobAlreadyExistsError`, so the backends cannot drift on what a duplicate means.
   */
  enqueue: (args: {
    wfId: string
    jobId: string
    /** Opaque payload; serialization is the caller's. */
    data: string
    groupId: string
    priority: number
    maxAttempts: number
    groupConcurrency: number
    /** Absolute epoch-ms to become ready at, or `null` for immediately ready. */
    runAt: number | null
  }) => Promise<'enqueued' | 'duplicate'>

  /**
   * The fused op — promote due delayed jobs, claim up to `want`, report both timers, hand back due
   * schedules — kept fused because it is one wake-loop pass, not because it is one statement:
   * Postgres reaches it as two statements pipelined into one round-trip (#12).
   *
   * The driver **mints the token**: it *is* the claim, so it is granted by whoever grants the
   * claim. Lua took it as `ARGV` only because Lua has no UUID, which is also why Redis mints one
   * per *batch* today; per-claim here frees a backend from that accident.
   */
  reserve: (args: {
    wfId: string
    /** Workflow ceiling. `null` = unlimited. The namespace ceiling came from the factory. */
    concurrency: number | null
    groupConcurrency: number
    /** Claim lifetime, renewed by {@link heartbeat}. */
    lockMs: number
    /** Max due delayed jobs promoted in this pass. */
    promoteBatchSize: number
    /** 0 is legal and load-bearing: a saturated worker still drives promotion and the cron tick. */
    want: number
  }) => Promise<{
    claims: { job: ClaimedJob; token: string }[]
    /** Ms until the next delayed job is due; `0` = already due, `null` = none. */
    msToNext: number | null
    /** Ms until the nearest cron occurrence; `0` = already due, `null` = none. */
    msToSchedule: number | null
    /** Capped at `promoteBatchSize`; carries pattern + tz so the tick costs no extra read (#14). */
    dueSchedules: DueSchedule[]
    /** A ceiling — ns, wf, or group — ended the batch, not an empty queue. */
    maxed: boolean
  }>

  /** Token-guarded commit. `'stale-token'` = the claim was recovered and re-reserved elsewhere. */
  complete: (args: {
    wfId: string
    jobId: string
    token: string
    /** The opaque result record the shared layer serialized; also the wake payload (#9). */
    record: string
    /** Lifetime of that record, in **ms** — the public `resultTtl` is seconds, converted by `Queue`. */
    resultTtlMs: number
    groupConcurrency: number
  }) => Promise<'committed' | 'stale-token'>

  /**
   * Token-guarded failure. The driver decides retry-vs-dead-letter from the job's own attempt
   * budget — `maxAttempts` was stamped at enqueue — and reports which happened.
   *
   * `keepFailed` is deliberately absent: #15 moved the trim onto the janitor for Postgres, because
   * failures cluster and an inline trim re-runs once per failed job. Redis follows, so the trim has
   * exactly one site.
   */
  fail: (args: {
    wfId: string
    jobId: string
    token: string
    reason: string
    stack: string
    /** Absolute epoch-ms for the retry, computed by the shared backoff policy. */
    retryAt: number
    resultTtlMs: number
    groupConcurrency: number
    /** `NonRecoverableError` — dead-letter now, skipping the remaining attempt budget. */
    nonRecoverable: boolean
  }) => Promise<'requeued' | 'dead-lettered' | 'stale-token'>

  /** Renew a claim. `'stale-token'` aborts the handler's `ctx.signal`. */
  heartbeat: (args: {
    wfId: string
    jobId: string
    token: string
    lockMs: number
  }) => Promise<'renewed' | 'stale-token'>

  /**
   * The janitor pass, renamed from `recoverStalled` because it has never only recovered stalls — it
   * also expires results and trims the failed list, and #15 gave Postgres four bounded units under
   * it. The shared layer keeps the local throttle; the driver owns the cross-process gate (Redis
   * `SET NX PX`, Postgres `pg_try_advisory_xact_lock` — a mutex, not a throttle).
   *
   * Returns nothing. #15 has Postgres looping each unit until drained, so any count would be a sum
   * over an unbounded number of transactions — a metric wearing a return value, and one the OTel
   * ticket should place rather than this one.
   */
  sweep: (args: {
    wfId: string
    groupConcurrency: number
    /** Recoveries a job survives before being dead-lettered. */
    maxStalledCount: number
    /** The cross-process gate's window. */
    stalledIntervalMs: number
    promoteBatchSize: number
    resultTtlMs: number
    /** Max retained failed jobs. */
    keepFailed: number
  }) => Promise<void>

  /**
   * CAS-on-`nextRun` cron fire: exactly-once across N workers, crash-safe because the *next*
   * occurrence is computed in JS before the call. `expectedRun` is matched by **equality** — `<`
   * double-fires (#14) — and a CAS that would not advance is refused (#19).
   */
  fireSchedule: (args: {
    wfId: string
    scheduleId: string
    /** The `nextRun` this fire was decided against; the CAS' expected value. */
    expectedRun: number
    /** The occurrence after this one, computed by the shared cron tick. */
    nextRun: number
    /** Id for the occurrence's job. */
    jobId: string
    groupConcurrency: number
  }) => Promise<'fired' | 'skipped' | 'stale'>

  /* ─────────────────────────────────── plain ops ─────────────────────────────────── */

  /** Persist one step's memo. Not token-guarded — a stale writer's job is already unclaimable. */
  setStepData: (args: {
    wfId: string
    jobId: string
    stepName: string
    value: string
  }) => Promise<void>

  /** The stored result record, or `null` if it never existed or has expired. */
  getResult: (args: { wfId: string; jobId: string }) => Promise<string | null>

  /** Register or replace a schedule, keyed by `(wfId, scheduleId)`. Preserves last-fire bookkeeping. */
  upsertSchedule: (args: {
    wfId: string
    scheduleId: string
    pattern: string
    /** IANA zone, resolved by the shared layer — never left to the backend's clock config. */
    tz: string
    data: string
    priority: number
    groupId: string
    skipIfRunning: boolean
    /** First occurrence, computed by the shared cron code. */
    nextRun: number
  }) => Promise<void>

  /** Idempotent. */
  removeSchedule: (args: { wfId: string; scheduleId: string }) => Promise<void>

  /** One op, contractually ordered by `(nextRun, scheduleId)` (#14). */
  getSchedules: (args: { wfId: string }) => Promise<ScheduleInfo[]>

  /** Point-in-time gauges, read on demand — no counters are maintained by either backend. */
  getMetrics: (args: { wfId: string }) => Promise<QueueMetrics>

  /* ──────────────────────────────── wake & notification ──────────────────────────────── */

  /**
   * A worker's park handle (#9): `BRPOP` on the two wake lists for Redis, a filtered `LISTEN` plus
   * poll backstop for Postgres. Per-worker because Redis' lists are point-to-point and need a
   * dedicated blocking connection — which is exactly what the handle owns.
   *
   * Synchronous, and repeatable: there is nothing to race, so Redis connects lazily inside the
   * first `wait()` exactly as `Worker.loop()` does today. `close()` is also `Worker.close()`'s
   * kick, which is why there is no `wake()` op.
   */
  wakeWaiter: (args: { wfId: string }) => WakeWaiter

  /**
   * A `wait()` caller's handle. Asynchronous, and one-shot: the subscribe must have *landed* before
   * the caller reads the result key, or the terminal publish can be missed. Per-job on both
   * backends — a notification with no listener is discarded server-side, so an idle per-job channel
   * costs nothing.
   */
  resultWaiter: (args: { wfId: string; jobId: string }) => Promise<ResultWaiter>
}

/* ─────────────────────────────────── value types ─────────────────────────────────── */

/**
 * A claimed job as the *backend* has it — not `ReservedJob`. `steps` is Redis' flat `[k, v, …]`
 * reply and Postgres' `jsonb` column, and both are one hop from this shape; the `Object.freeze` and
 * the `Map` build are identical work that would otherwise be copied into every driver. The public
 * type is minted once, by the shared layer.
 */
export type ClaimedJob = {
  id: string
  groupId: string
  data: string
  attemptsMade: number
  priority: number
  steps: Record<string, string>
}

export type DueSchedule = {
  scheduleId: string
  /** The `nextRun` that made it due — the CAS' expected value. */
  nextRun: number
  pattern: string
  tz: string
}

export type WakeWaiter = {
  /** Resolves on new work, a release, or `timeoutMs`. Never rejects — a wake is best-effort. */
  wait: (args: { timeoutMs: number }) => Promise<void>
  close: () => Promise<void>
}

export type ResultWaiter = {
  /**
   * The terminal publish's payload: the result record itself when it fits, else a bare marker the
   * shared layer re-reads through {@link WorkflowDriver.getResult} (#9's 7999-byte cap, and the
   * rolling-deploy fallback that already exists).
   */
  published: Promise<string>
  close: () => Promise<void>
}
