import type Redis from 'ioredis'
import type { Queue } from './queue'
import type { QueueRedis } from './scripts'
import type { JobContext, ReservedJob, WorkerOptions } from './types'
import { randomUUID } from 'node:crypto'
import { expBackoff } from './backoff'
import { NonRecoverableError } from './errors'
import { localTimeZone, nextRunMs } from './schedule'

export type WorkerHandler = (job: ReservedJob, ctx: JobContext) => Promise<string> | string

/**
 * Max jobs claimed per `reserve`. Redis runs Lua single-threaded, so the batch is a pause for
 * every* client sharing the instance — a worker with `concurrency: 500` must not turn one wake
 * into thousands of serialized commands. Filling a larger `concurrency` just takes more passes,
 * which cost one round-trip each and keep the instance responsive in between.
 */
const RESERVE_BATCH_CAP = 64

/** Redis' `[field, value, …]` hash reply as a Map. */
function flatToMap(flat: string[]): Map<string, string> {
  const map = new Map<string, string>()
  for (let i = 0; i < flat.length; i += 2) map.set(flat[i]!, flat[i + 1]!)
  return map
}

interface Claim {
  job: ReservedJob
  token: string
}

interface ReserveResult {
  claims: Claim[]
  /** Ms until the next delayed job is due (`-1` none, `0` already due). */
  msToNext: number
  /** Ms until the nearest cron occurrence (`-1` none, `0` already due). */
  msToSchedule: number
  /** `[scheduleId, score, …]` of the schedules due right now. */
  dueSchedules: string[]
  /** A concurrency ceiling, not an empty queue, ended the batch. */
  maxed: boolean
}

/**
 * Runs jobs for one workflow. A JS-local semaphore (`concurrency`) is the per-worker cap —
 * no Redis round-trip. Each pass makes exactly one `reserve` call that claims a batch (capped at
 * `RESERVE_BATCH_CAP`) and reports the delayed/cron timers, then parks one of two ways:
 *
 * - **slots free** — `BRPOP wf:wake ns:wake <safetyTimeout>` on its own connection. Enqueue and
 *   every release `LPUSH` those wake lists, so new work anywhere re-drives the loop.
 * - **every slot busy** — an in-process wait for a slot to free. Nothing Redis could say is
 *   actionable while saturated, and the next actionable event is local, so this costs no
 *   round-trip; a completion re-drives the loop the instant it lands.
 */
export class Worker {
  private readonly concurrency: number
  private readonly lockMs: number
  private readonly heartbeatInterval: number
  private readonly maxStalledCount: number
  private readonly stalledInterval: number
  private readonly safetyTimeout: number
  private readonly promoteBatchSize: number
  private readonly backoff: (attempt: number) => number
  private readonly keepFailed: number
  private readonly onError: (error: unknown) => void
  private readonly onFailed: (job: ReservedJob, error: unknown) => void

  private readonly blockingRedis: Redis
  private readonly redis: QueueRedis
  private readonly wfWake: string
  private readonly nsWake: string

  private inFlight = 0
  private closing = false
  private lastStalledScan = 0
  private wakeSlot?: () => void
  private readonly inProgress = new Set<Promise<void>>()
  private readonly loopPromise: Promise<void>

  constructor(
    private readonly queue: Queue,
    readonly handler: WorkerHandler,
    opts?: WorkerOptions,
  ) {
    this.concurrency = opts?.concurrency ?? 1
    this.lockMs = opts?.lockMs ?? 30_000
    // Derived, not exposed: renew ~3× within lockMs, capped at 10s so long locks still
    // renew often enough that a single missed tick can't stall a healthy job.
    this.heartbeatInterval = Math.min(this.lockMs / 3, 10_000)
    this.maxStalledCount = opts?.maxStalledCount ?? 1
    this.stalledInterval = opts?.stalledInterval ?? 30_000
    this.safetyTimeout = opts?.safetyTimeout ?? 5
    this.promoteBatchSize = opts?.promoteBatchSize ?? 500
    this.backoff = opts?.backoff ?? expBackoff()
    this.keepFailed = opts?.keepFailed ?? 100
    this.onError = opts?.onError ?? ((error) => queue.logger?.error?.(error))
    this.onFailed = opts?.onFailed ?? ((_job, error) => queue.logger?.error?.(error))

    this.redis = queue.redis
    this.blockingRedis = queue.redis.duplicate()
    this.wfWake = `${queue.prefix}:${queue.id}:wake`
    this.nsWake = `${queue.prefix}:ns:${queue.ns.id}:wake`

    this.loopPromise = this.loop()
  }

  private async loop(): Promise<void> {
    if (this.blockingRedis.status === 'wait') await this.blockingRedis.connect()
    while (!this.closing) {
      // One round-trip per pass: claim a batch, promote due delayed jobs, and read both timers.
      // A locally saturated worker still calls it with `want = 0` — promotion and the cron tick
      // must keep happening while every slot is busy.
      const want = Math.min(this.concurrency - this.inFlight, RESERVE_BATCH_CAP)
      const res = await this.reserve(want)
      // Start what we claimed even while closing: these jobs hold real locks and slots in Redis,
      // and dropping them here would strand them until stalled-recovery — on every clean
      // shutdown. `close` drains `inProgress`, so starting them is what makes the drain honest.
      for (const claim of res.claims) this.start(claim)
      if (this.closing) break

      // Fire any due cron occurrences into waiting — no separate poller; the tick folds into
      // this same wake loop. Firing kicks `wake`, so the next pass picks the occurrence up.
      if (res.dueSchedules.length > 0) {
        await this.tickSchedules(res.dueSchedules)
        continue
      }
      const saturated = this.inFlight >= this.concurrency
      // The batch cap, not an empty queue, ended this pass — go straight back for the next chunk.
      if (!saturated && res.claims.length === want) continue
      // Due work remained past the promote cap — re-reserve at once to drain the next chunk,
      // unless a ceiling (not an empty queue) is what stopped us, which would spin.
      if (!saturated && res.msToNext === 0 && !res.maxed) continue
      // Block until woken (new work / freed slot / enqueued delay), the next delayed job or
      // schedule comes due, or the safety re-poll fires — whichever is sooner. The block is the
      // timer for both delayed jobs and schedules.
      // Only *future* timers count: a `0` here means due work we just decided we cannot take
      // (capped out), and folding it in would mean `brpop … 0` — which blocks forever, not
      // "immediately" — or a hot `waitForSlot(0)` spin. Falling back to `safetyTimeout` is what
      // the pre-batch code did in exactly this state.
      const nearest = [res.msToNext, res.msToSchedule].filter((m) => m > 0)
      const timeout =
        nearest.length === 0
          ? this.safetyTimeout
          : Math.min(Math.min(...nearest) / 1000, this.safetyTimeout)
      // With every slot busy there is nothing Redis could tell us that we could act on, and the
      // next thing we *can* act on — a slot freeing — is local. Park on it and skip the round
      // trip entirely; a completion re-drives the loop the instant it lands.
      if (saturated) await this.waitForSlot(timeout * 1000)
      else await this.blockingRedis.brpop(this.wfWake, this.nsWake, timeout)
      // Wake-loop re-poll is the idle-worker stalled-recovery trigger — no dedicated poller.
      if (!this.closing) void this.recoverStalled()
    }
  }

  /**
   * The thin JS cron tick, folded into the wake loop — no poller. `reserve` hands back the due
   * schedules (`[scheduleId, score, …]`), this computes each one's `nextRun(now)` via Croner and
   * calls the `fireSchedule` CAS script with the score it saw. CAS-on-score = exactly-once across
   * N workers; computing next in JS *before* the call = crash-safe. A backlog after downtime
   * collapses to one fire because `nextRun(now)` jumps forward. Errors are best-effort logged.
   */
  private async tickSchedules(due: string[]): Promise<void> {
    try {
      for (let i = 0; i < due.length && !this.closing; i += 2) {
        const scheduleId = due[i]!
        const expectedScore = due[i + 1]!
        const scheduleKey = `${this.queue.prefix}:${this.queue.id}:schedule:${scheduleId}`
        const [pattern, tz] = await this.redis.hmget(scheduleKey, 'pattern', 'tz')
        if (!pattern) continue // removed concurrently
        const next = nextRunMs(pattern, tz ?? localTimeZone(), new Date())
        if (next === null) continue // no future occurrence
        await this.redis.fireSchedule(
          this.queue.prefix,
          this.queue.id,
          this.queue.ns.id,
          scheduleId,
          expectedScore,
          next,
          randomUUID(),
          1,
          this.queue.groupConcurrency,
        )
      }
    } catch (err) {
      this.onError(err)
    }
  }

  /**
   * Fire the throttled stalled-recovery scan. Redis owns the cross-process gate; the local
   * timestamp keeps this worker from paying a round-trip per wake to be told so — during a fast
   * drain that is one saved command every time round the loop. Either way some worker scans
   * within `stalledInterval`, which is the only guarantee recovery makes.
   */
  private async recoverStalled(): Promise<void> {
    const now = Date.now()
    if (now - this.lastStalledScan < this.stalledInterval) return
    this.lastStalledScan = now
    try {
      await this.redis.recoverStalled(
        this.queue.prefix,
        this.queue.id,
        this.queue.groupConcurrency,
        this.maxStalledCount,
        this.stalledInterval,
        this.promoteBatchSize,
        this.queue.resultTtl,
        this.keepFailed,
      )
    } catch (err) {
      // The scan never happened, so it must not count against the local throttle.
      this.lastStalledScan = 0
      this.onError(err)
    }
  }

  /** Claim up to `want` jobs (0 = report only) and read back both wake timers, in one call. */
  private async reserve(want: number): Promise<ReserveResult> {
    const [jobs, msToNext, msToSchedule, dueSchedules, maxed] = await this.redis.reserve(
      this.queue.prefix,
      this.queue.id,
      this.queue.ns.id,
      this.queue.ns.concurrency,
      this.queue.concurrency,
      this.queue.groupConcurrency,
      this.lockMs,
      randomUUID(),
      this.promoteBatchSize,
      want,
    )
    return {
      claims: jobs.map(([id, groupId, data, attempts, priority, token, steps]) => ({
        job: Object.freeze({
          id,
          groupId,
          data,
          attemptsMade: Number(attempts),
          priority: Number(priority),
          steps: flatToMap(steps),
        }) as ReservedJob,
        token,
      })),
      msToNext: Number(msToNext),
      msToSchedule: Number(msToSchedule),
      dueSchedules,
      maxed: maxed === 1,
    }
  }

  private start(claim: Claim): void {
    this.inFlight++
    const promise = this.process(claim).finally(() => {
      this.inFlight--
      this.inProgress.delete(promise)
      this.wakeSlot?.()
    })
    this.inProgress.add(promise)
  }

  /**
   * Park until a slot frees locally (or `timeoutMs` elapses) — the zero-round-trip block. There
   * is only ever one waiter, the single wake loop, so this is one nullable callback rather than
   * a waiter list.
   */
  private async waitForSlot(timeoutMs: number): Promise<void> {
    const { promise, resolve } = Promise.withResolvers<void>()
    const timer = setTimeout(resolve, timeoutMs)
    this.wakeSlot = () => {
      clearTimeout(timer)
      resolve()
    }
    try {
      await promise
    } finally {
      this.wakeSlot = undefined
    }
  }

  private async process(claim: Claim): Promise<void> {
    const controller = new AbortController()
    const stopHeartbeat = this.startHeartbeat(claim, controller)
    try {
      const result = await this.handler(claim.job, { signal: controller.signal })
      // Abort-on-lost-claim: the heartbeat aborted because the claim was recovered
      // and re-reserved elsewhere — drop the job without committing. The token-guard on
      // complete/fail makes those no-ops anyway, but skipping avoids the wasted round-trip.
      if (controller.signal.aborted) return
      const record = JSON.stringify({ state: 'completed', value: result })
      await this.redis.complete(
        this.queue.prefix,
        this.queue.id,
        claim.job.id,
        claim.token,
        record,
        this.queue.resultTtl,
        this.queue.groupConcurrency,
      )
    } catch (err) {
      if (controller.signal.aborted) return
      await this.fail(claim, err)
    } finally {
      stopHeartbeat()
    }
  }

  /**
   * Renew the claim on a derived timer. A token-CAS renew returning 0 (the claim was
   * recovered + re-reserved elsewhere) or erroring past `lockMs` aborts `ctx.signal` and stops
   * the timer, so a cooperative handler can bail and `process` drops the job. Each successful
   * tick also fires the throttled stalled scan — the busy-worker recovery trigger.
   * Returns a stop function.
   */
  private startHeartbeat(claim: Claim, controller: AbortController): () => void {
    let lastRenew = Date.now()
    const timer = setInterval(() => {
      void (async () => {
        try {
          const ok = await this.redis.heartbeat(
            this.queue.prefix,
            this.queue.id,
            claim.job.id,
            claim.token,
            this.lockMs,
          )
          if (ok === 0) {
            controller.abort()
            clearInterval(timer)
            return
          }
          lastRenew = Date.now()
          void this.recoverStalled()
        } catch (err) {
          // A transient renew error is tolerated until the lock could actually have expired.
          if (Date.now() - lastRenew >= this.lockMs) {
            controller.abort()
            clearInterval(timer)
          }
          this.onError(err)
        }
      })()
    }, this.heartbeatInterval)
    return () => clearInterval(timer)
  }

  /**
   * Route a handler throw through the token-guarded `fail` script: it increments `attempts`
   * and either requeues via the delayed ZSET after `backoff` (releasing all slots) or
   * dead-letters. `attemptsMade` counts prior attempts, so the attempt that just failed is
   * `attemptsMade + 1`. Backoff `runAt` is computed here (JS) and stored verbatim as the score.
   */
  private async fail(claim: Claim, err: unknown): Promise<void> {
    const error = err instanceof Error ? err : new Error(String(err))
    // Best-effort local failure notification — fires on every handler failure, retryable or
    // terminal.
    this.onFailed(claim.job, error)
    const runAt = Date.now() + this.backoff(claim.job.attemptsMade + 1)
    try {
      await this.redis.fail(
        this.queue.prefix,
        this.queue.id,
        claim.job.id,
        claim.token,
        error.message,
        error.stack ?? '',
        runAt,
        this.queue.resultTtl,
        this.queue.groupConcurrency,
        this.keepFailed,
        err instanceof NonRecoverableError ? 1 : 0,
      )
    } catch (failErr) {
      this.onError(failErr)
    }
  }

  /** Stop accepting work, drain in-flight jobs, then quit the blocking connection. */
  async close(): Promise<void> {
    this.closing = true
    // Unblock the loop so it can observe `closing`, whichever way it is parked.
    this.wakeSlot?.()
    await this.redis.lpush(this.wfWake, '1')
    await this.redis.ltrim(this.wfWake, 0, 0)
    await this.loopPromise
    await Promise.all(this.inProgress)
    this.blockingRedis.disconnect()
  }
}
