import type Redis from 'ioredis'
import type { AttemptOutcome, WorkerMetrics } from './metrics'
import type { Queue } from './queue'
import type { QueueRedis } from './scripts'
import type { JobContext, ReservedJob, WorkerOptions } from './types'
import { randomUUID } from 'node:crypto'
import { expBackoff } from './backoff'
import { NonRecoverableError, RateLimitError } from './errors'
import { localTimeZone, nextRunMs } from './schedule'

export type WorkerHandler = (job: ReservedJob, ctx: JobContext) => Promise<string> | string

const RESERVE_BATCH_CAP = 64

const redisRetryBackoff = expBackoff({ base: 250, cap: 5000 })

function flatToMap(flat: string[]): Map<string, string> {
  const map = new Map<string, string>()
  for (let i = 0; i < flat.length; i += 2) map.set(flat[i]!, flat[i + 1]!)
  return map
}

interface Claim {
  job: ReservedJob
  token: string
  ageMs: number
}

interface ReserveResult {
  claims: Claim[]
  msToNext: number
  msToSchedule: number
  dueSchedules: string[]
  maxed: boolean
  msToLimit: number
}

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
    private readonly metrics?: WorkerMetrics,
  ) {
    this.concurrency = opts?.concurrency ?? 1
    this.lockMs = opts?.lockMs ?? 30_000
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
    let failures = 0
    while (!this.closing) {
      try {
        if (this.blockingRedis.status === 'wait') await this.blockingRedis.connect()
        await this.pass()
        failures = 0
      } catch (err) {
        this.onError(err)
        if (!this.closing) await this.waitForSlot(redisRetryBackoff(++failures))
      }
    }
  }

  private async pass(): Promise<void> {
    const want = Math.min(this.concurrency - this.inFlight, RESERVE_BATCH_CAP)
    const res = await this.reserve(want)
    for (const claim of res.claims) this.start(claim)
    if (this.closing) return

    if (res.dueSchedules.length > 0) {
      await this.tickSchedules(res.dueSchedules)
      return
    }
    const saturated = this.inFlight >= this.concurrency
    if (!saturated && res.claims.length === want) return
    if (!saturated && res.msToNext === 0 && !res.maxed) return
    const nearest = [res.msToNext, res.msToSchedule, res.msToLimit].filter((m) => m > 0)
    const timeout =
      nearest.length === 0
        ? this.safetyTimeout
        : Math.min(Math.min(...nearest) / 1000, this.safetyTimeout)
    if (saturated || res.msToLimit > 0) await this.waitForSlot(timeout * 1000)
    else await this.blockingRedis.brpop(this.wfWake, this.nsWake, timeout)
    if (!this.closing) void this.recoverStalled()
  }

  private async tickSchedules(due: string[]): Promise<void> {
    try {
      for (let i = 0; i < due.length && !this.closing; i += 2) {
        const scheduleId = due[i]!
        const expectedScore = due[i + 1]!
        const scheduleKey = `${this.queue.prefix}:${this.queue.id}:schedule:${scheduleId}`
        const [pattern, tz] = await this.redis.hmget(scheduleKey, 'pattern', 'tz')
        if (!pattern) continue
        const from = new Date(Math.max(Date.now(), Number(expectedScore)))
        const next = nextRunMs(pattern, tz ?? localTimeZone(), from)
        if (next === null) continue
        const [fired] = await this.redis.fireSchedule(
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
        if (fired === 'fired')
          this.metrics?.fired(scheduleId, Math.max(0, Date.now() - Number(expectedScore)))
      }
    } catch (err) {
      this.onError(err)
    }
  }

  private async recoverStalled(): Promise<void> {
    const now = Date.now()
    if (now - this.lastStalledScan < this.stalledInterval) return
    this.lastStalledScan = now
    try {
      const [requeued, deadLettered] = await this.redis.recoverStalled(
        this.queue.prefix,
        this.queue.id,
        this.queue.groupConcurrency,
        this.maxStalledCount,
        this.stalledInterval,
        this.promoteBatchSize,
        this.queue.resultTtl,
        this.keepFailed,
      )
      this.metrics?.stalled(requeued, deadLettered)
    } catch (err) {
      this.lastStalledScan = 0
      this.onError(err)
    }
  }

  private async reserve(want: number): Promise<ReserveResult> {
    const [jobs, msToNext, msToSchedule, dueSchedules, maxed, msToLimit] = await this.redis.reserve(
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
      ...this.queue.rateLimitArgs,
    )
    return {
      claims: jobs.map(
        ([id, groupId, data, attempts, priority, token, steps, queueWaitMs, ageMs]) => ({
          job: Object.freeze({
            id,
            groupId,
            data,
            attemptsMade: Number(attempts),
            priority: Number(priority),
            steps: flatToMap(steps),
            queueWaitMs,
          }),
          token,
          ageMs,
        }),
      ),
      msToNext: Number(msToNext),
      msToSchedule: Number(msToSchedule),
      dueSchedules,
      maxed: maxed === 1,
      msToLimit,
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
    this.metrics?.claimed(claim.job.queueWaitMs)
    const stopHeartbeat = this.startHeartbeat(claim, controller)
    const start = performance.now()
    let durationMs = 0
    const settled = (outcome: AttemptOutcome) =>
      this.metrics?.attempt(
        outcome,
        durationMs,
        claim.job.attemptsMade + 1,
        claim.ageMs + performance.now() - start,
      )
    try {
      const started = this.queue
        .publish(claim.job.id, {
          type: 'started',
          attempt: claim.job.attemptsMade + 1,
        })
        .catch(this.onError)
      let result: string
      try {
        result = await this.handler(claim.job, { signal: controller.signal })
      } finally {
        durationMs = performance.now() - start
      }
      await started
      if (controller.signal.aborted) return
      const record = JSON.stringify({ state: 'completed', value: result })
      const completed = await this.redis.complete(
        this.queue.prefix,
        this.queue.id,
        claim.job.id,
        claim.token,
        record,
        this.queue.resultTtl,
        this.queue.groupConcurrency,
      )
      if (completed === 1) settled('completed')
    } catch (err) {
      if (err instanceof RateLimitError) {
        try {
          const requeued = await this.redis.rateLimit(
            this.queue.prefix,
            this.queue.id,
            this.queue.ns.id,
            claim.job.id,
            claim.token,
            err.ms,
            err.limiter,
            this.queue.groupConcurrency,
          )
          if (requeued === 1) settled('rate_limited')
        } catch (err_) {
          this.onError(err_)
        }
      } else if (!controller.signal.aborted) {
        const failed = await this.fail(claim, err)
        if (failed !== 0) settled(failed === 2 ? 'retried' : 'failed')
      }
    } finally {
      stopHeartbeat()
    }
  }

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

  private async fail(claim: Claim, err: unknown): Promise<number> {
    const error = err instanceof Error ? err : new Error(String(err))
    this.onFailed(claim.job, error)
    const runAt = Date.now() + this.backoff(claim.job.attemptsMade + 1)
    try {
      return await this.redis.fail(
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
      return 0
    }
  }

  async close(): Promise<void> {
    this.closing = true
    this.wakeSlot?.()
    await this.redis.lpush(this.wfWake, '1')
    await this.redis.ltrim(this.wfWake, 0, 0)
    await this.loopPromise
    await Promise.all(this.inProgress)
    this.blockingRedis.disconnect()
  }
}
