import type { Namespace } from './namespace'
import type { QueueRedis } from './scripts'
import type {
  AddOptions,
  QueueMetrics,
  QueueOptions,
  ScheduleInfo,
  ScheduleOptions,
  WaitOptions,
  WorkerOptions,
  WorkflowLogger,
} from './types'
import { randomUUID } from 'node:crypto'
import { JobAlreadyExistsError, ResultExpiredError, TimeoutError } from './errors'
import { localTimeZone, nextRunMs } from './schedule'
import { PMAX, UNLIMITED } from './scripts'
import { Worker } from './worker'

/**
 * One workflow instance. Enqueues jobs, mints workers, and blocks on `wait()`. `data` is
 * an opaque string end-to-end; serialization is the caller's job.
 */
export class Queue {
  readonly id: string
  readonly concurrency: number
  readonly groupConcurrency: number
  readonly resultTtl: number
  readonly redis: QueueRedis

  private readonly workers = new Set<Worker>()

  constructor(
    readonly ns: Namespace,
    opts: QueueOptions,
  ) {
    this.id = opts.id
    this.concurrency = opts.concurrency ?? UNLIMITED
    this.groupConcurrency = opts.groupConcurrency ?? 1
    this.resultTtl = opts.resultTtl ?? 300
    this.redis = ns.redis
  }

  get prefix(): string {
    return this.ns.prefix
  }

  get logger(): WorkflowLogger | undefined {
    return this.ns.logger
  }

  /** Enqueue an immediate job. Throws `JobAlreadyExistsError` on a live id collision. */
  async add(data: string, opts?: AddOptions): Promise<{ id: string; groupId: string }> {
    const id = opts?.jobId ?? randomUUID()
    const groupId = opts?.groupId ?? randomUUID()
    const priority = opts?.priority ?? 0
    // An out-of-range or fractional priority would corrupt score packing / ordering.
    this.validatePriority(priority)
    if (opts?.runAt !== undefined && opts?.runIn !== undefined)
      throw new Error('`runAt` and `runIn` are mutually exclusive')
    try {
      await this.redis.enqueue(
        this.prefix,
        this.id,
        this.ns.id,
        id,
        data,
        groupId,
        priority,
        opts?.maxAttempts ?? 1,
        this.groupConcurrency,
        opts?.runAt ?? -1,
        opts?.runIn ?? -1,
      )
    } catch (err) {
      if (err instanceof Error && err.message.includes('JobAlreadyExists'))
        throw new JobAlreadyExistsError(id)
      throw err
    }
    return { id, groupId }
  }

  worker(handler: Worker['handler'], opts?: WorkerOptions): Worker {
    const worker = new Worker(this, handler, opts)
    this.workers.add(worker)
    return worker
  }

  /**
   * Block until `jobId` finishes and return its raw result string. Subscribes to the
   * done channel *before* reading the result key, so the terminal publish can't be missed.
   * Throws `TimeoutError`, `ResultExpiredError`, or the job's own failure.
   */
  async wait(jobId: string, opts?: WaitOptions): Promise<string> {
    const channel = `${this.prefix}:${this.id}:done:${jobId}`
    const resultKey = `${this.prefix}:${this.id}:result:${jobId}`

    let notify!: () => void
    const notified = new Promise<void>((resolve) => {
      notify = resolve
    })
    await this.ns.addWaiter(channel, notify)
    try {
      const first = await this.redis.get(resultKey)
      if (first !== null) return this.parseResult(first)

      if (opts?.timeoutMs === undefined) {
        await notified
      } else {
        let timer: NodeJS.Timeout
        const timedOut = new Promise<'timeout'>((resolve) => {
          timer = setTimeout(() => resolve('timeout'), opts.timeoutMs)
        })
        const outcome = await Promise.race([notified.then(() => 'done' as const), timedOut])
        clearTimeout(timer!)
        if (outcome === 'timeout') throw new TimeoutError(jobId)
      }

      const result = await this.redis.get(resultKey)
      if (result === null) throw new ResultExpiredError(jobId)
      return this.parseResult(result)
    } finally {
      await this.ns.removeWaiter(channel, notify)
    }
  }

  /** Persist a step's data (opaque string) — a single atomic `HSET` on the `:steps` hash. */
  async setStepData(jobId: string, stepName: string, value: string): Promise<void> {
    await this.redis.hset(`${this.prefix}:${this.id}:j:${jobId}:steps`, stepName, value)
  }

  /** Read a step's data — a single `HGET`, `null` on miss. Opaque string, no deserialization. */
  async getStepData(jobId: string, stepName: string): Promise<string | null> {
    return this.redis.hget(`${this.prefix}:${this.id}:j:${jobId}:steps`, stepName)
  }

  private parseResult(raw: string): string {
    const record = JSON.parse(raw) as {
      state: string
      value?: string
      reason?: string
      stack?: string
    }
    if (record.state === 'failed') {
      const error = new Error(record.reason ?? 'job failed')
      if (record.stack) error.stack = record.stack
      throw error
    }
    return record.value ?? ''
  }

  /**
   * Register (or idempotently replace) a recurring cron schedule keyed by `(wfId, scheduleId)`.
   * Structurally cannot duplicate: the `schedule:<id>` hash and the single `schedules:due` member
   * are addressed by `scheduleId`, so upserting the same id twice overwrites in place — one hash,
   * one due entry — with no `removeRepeatingJob` dance. The timezone is captured from the queuer's
   * local IANA zone unless overridden. `lastJobId`/`lastFireAt` are preserved across upserts.
   */
  async upsertSchedule(scheduleId: string, opts: ScheduleOptions): Promise<void> {
    const tz = opts.tz ?? localTimeZone()
    const priority = opts.priority ?? 0
    this.validatePriority(priority)
    const next = nextRunMs(opts.pattern, tz)
    if (next === null) throw new Error(`cron pattern has no next occurrence: ${opts.pattern}`)
    const scheduleKey = `${this.prefix}:${this.id}:schedule:${scheduleId}`
    const dueKey = `${this.prefix}:${this.id}:schedules:due`
    await this.redis
      .multi()
      .hset(scheduleKey, {
        pattern: opts.pattern,
        tz,
        data: opts.data,
        priority,
        groupId: opts.groupId ?? scheduleId,
        skipIfRunning: (opts.skipIfRunning ?? true) ? '1' : '0',
        active: '1',
      })
      .zadd(dueKey, next, scheduleId)
      .exec()
  }

  /** Remove a schedule by id: drops its due entry and its record hash. Idempotent. */
  async removeSchedule(scheduleId: string): Promise<void> {
    const scheduleKey = `${this.prefix}:${this.id}:schedule:${scheduleId}`
    const dueKey = `${this.prefix}:${this.id}:schedules:due`
    await this.redis.multi().del(scheduleKey).zrem(dueKey, scheduleId).exec()
  }

  /** List registered schedules with their next-fire time and last-fire bookkeeping. */
  async getSchedules(): Promise<ScheduleInfo[]> {
    const dueKey = `${this.prefix}:${this.id}:schedules:due`
    const entries = await this.redis.zrange(dueKey, 0, -1, 'WITHSCORES')
    const schedules: ScheduleInfo[] = []
    for (let i = 0; i < entries.length; i += 2) {
      const scheduleId = entries[i]!
      const nextRun = Number(entries[i + 1])
      const scheduleKey = `${this.prefix}:${this.id}:schedule:${scheduleId}`
      const [pattern, tz, lastFireAt, lastJobId] = await this.redis.hmget(
        scheduleKey,
        'pattern',
        'tz',
        'lastFireAt',
        'lastJobId',
      )
      schedules.push({
        scheduleId,
        pattern: pattern ?? '',
        tz: tz ?? '',
        nextRun,
        lastFireAt: lastFireAt ? Number(lastFireAt) : null,
        lastJobId: lastJobId ?? null,
      })
    }
    return schedules
  }

  /**
   * Point-in-time queue-depth gauges, read on demand — no counters are maintained. `active` and
   * `delayed` are O(1) `ZCARD`s; `waiting` sums the per-group waiting ZSETs over the `groups`
   * membership SET (O(groups)), which is kept in exact step with the waiting jobs so a drained
   * group is never enumerated.
   */
  async getMetrics(): Promise<QueueMetrics> {
    const wf = `${this.prefix}:${this.id}`
    const [active, delayed, groups] = await Promise.all([
      this.redis.zcard(`${wf}:active`),
      this.redis.zcard(`${wf}:delayed`),
      this.redis.smembers(`${wf}:groups`),
    ])
    let waiting = 0
    if (groups.length > 0) {
      const pipeline = this.redis.pipeline()
      for (const groupId of groups) pipeline.zcard(`${wf}:g:${groupId}:jobs`)
      const results = await pipeline.exec()
      for (const [, count] of results ?? []) waiting += Number(count ?? 0)
    }
    return { active, waiting, delayed }
  }

  /** Guard the range that keeps the packed score exact in a ZSET double. */
  private validatePriority(priority: number): void {
    if (!Number.isInteger(priority) || priority < 0 || priority > PMAX)
      throw new RangeError(`priority must be an integer in 0…${PMAX}, got ${priority}`)
  }

  /** Releases queue-owned handles: closes workers and unsubscribes any `wait()` channels. */
  async close(): Promise<void> {
    await Promise.all([...this.workers].map(async (w) => w.close()))
    this.workers.clear()
  }
}
