import type { WorkerMetrics } from './metrics'
import type { Namespace } from './namespace'
import type { QueueRedis } from './scripts'
import type {
  AddOptions,
  QueueEvent,
  QueueMetrics,
  QueueOptions,
  ScheduleInfo,
  ScheduleOptions,
  WaitOptions,
  WatchOptions,
  WorkerOptions,
  WorkflowLogger,
} from './types'
import { randomUUID } from 'node:crypto'
import { JobAlreadyExistsError, ResultExpiredError, TimeoutError } from './errors'
import { localTimeZone, nextRunMs } from './schedule'
import { PMAX, UNLIMITED } from './scripts'
import { Worker } from './worker'

export class Queue {
  private readonly workers = new Set<Worker>()

  readonly id: string
  readonly concurrency: number
  readonly groupConcurrency: number
  readonly resultTtl: number
  readonly redis: QueueRedis
  readonly rateLimitArgs: (string | number)[]

  constructor(
    readonly ns: Namespace,
    opts: QueueOptions,
  ) {
    this.id = opts.id
    this.concurrency = opts.concurrency ?? UNLIMITED
    this.groupConcurrency = opts.groupConcurrency ?? 1
    this.resultTtl = opts.resultTtl ?? 300
    this.redis = ns.redis
    const names = opts.rateLimiters ?? []
    this.rateLimitArgs = [
      names.length,
      ...names.flatMap((name) => {
        const budget = ns.rateLimiters[name]!
        return [name, budget.limit, budget.window]
      }),
    ]
  }

  private validatePriority(priority: number): void {
    if (!Number.isSafeInteger(priority) || priority < 0 || priority > PMAX)
      throw new RangeError(`priority must be an integer in 0…${PMAX}, got ${priority}`)
  }

  private async parsePublished(jobId: string, published: string): Promise<QueueEvent> {
    if (!published.startsWith('{')) {
      const stored = await this.redis.get(`${this.prefix}:${this.id}:result:${jobId}`)
      if (stored === null) throw new ResultExpiredError(jobId)
      return this.parsePublished(jobId, stored)
    }
    const record: unknown = JSON.parse(published)
    if (typeof record !== 'object' || record === null)
      throw new Error(`Invalid job event for ${jobId}`)
    if (
      'type' in record &&
      record.type === 'started' &&
      'attempt' in record &&
      typeof record.attempt === 'number' &&
      Number.isSafeInteger(record.attempt) &&
      record.attempt > 0
    )
      return { type: 'started', attempt: record.attempt }
    if (
      'type' in record &&
      record.type === 'progress' &&
      'data' in record &&
      typeof record.data === 'string'
    )
      return { type: 'progress', data: record.data }
    if (
      'state' in record &&
      record.state === 'failed' &&
      'reason' in record &&
      typeof record.reason === 'string' &&
      (!('stack' in record) || typeof record.stack === 'string')
    ) {
      const error = new Error(record.reason)
      if ('stack' in record && typeof record.stack === 'string' && record.stack)
        // eslint-disable-next-line unicorn/no-error-property-assignment
        error.stack = record.stack
      return { type: 'failed', error }
    }
    if (
      'state' in record &&
      record.state === 'completed' &&
      'value' in record &&
      typeof record.value === 'string'
    )
      return { type: 'completed', output: record.value }
    throw new Error(`Invalid job event for ${jobId}`)
  }

  get prefix(): string {
    return this.ns.prefix
  }

  get logger(): WorkflowLogger | undefined {
    return this.ns.logger
  }

  async add(data: string, opts?: AddOptions): Promise<{ id: string; groupId: string }> {
    const id = opts?.jobId ?? randomUUID()
    const groupId = opts?.groupId ?? randomUUID()
    const priority = opts?.priority ?? 0
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

  worker(handler: Worker['handler'], opts?: WorkerOptions, metrics?: WorkerMetrics): Worker {
    const worker = new Worker(this, handler, opts, metrics)
    this.workers.add(worker)
    return worker
  }

  async wait(jobId: string, opts?: WaitOptions): Promise<string> {
    const controller = new AbortController()
    const timer =
      opts?.timeoutMs === undefined
        ? undefined
        : setTimeout(() => controller.abort(new TimeoutError(jobId)), opts.timeoutMs)
    try {
      const events = await this.watch(jobId, { signal: controller.signal })
      for await (const event of events) {
        if (event.type === 'completed') return event.output
        if (event.type === 'failed') throw event.error
      }
      throw new ResultExpiredError(jobId)
    } finally {
      clearTimeout(timer)
    }
  }

  async watch(jobId: string, opts?: WatchOptions): Promise<ReadableStream<QueueEvent>> {
    const channel = `${this.prefix}:${this.id}:done:${jobId}`
    const resultKey = `${this.prefix}:${this.id}:result:${jobId}`
    let controller!: ReadableStreamDefaultController<QueueEvent>
    let done = false
    let cleaned = false
    let processing = Promise.resolve()
    let notify!: (raw: string) => void
    let abort!: () => void

    const cleanup = async () => {
      if (cleaned) return
      cleaned = true
      opts?.signal?.removeEventListener('abort', abort)
      await this.ns.removeWaiter(channel, notify)
    }
    const fail = (error: unknown) => {
      if (done) return
      done = true
      controller.error(error)
      void cleanup().catch((err) => this.logger?.error?.(err))
    }
    const emit = async (raw: string) => {
      if (done) return
      const event = await this.parsePublished(jobId, raw)
      controller.enqueue(event)
      if (event.type === 'completed' || event.type === 'failed') {
        done = true
        controller.close()
        await cleanup()
      }
    }
    notify = (raw: string) => {
      processing = processing.then(async () => emit(raw)).catch(fail)
    }
    abort = () => fail(opts?.signal?.reason)
    const events = new ReadableStream<QueueEvent>({
      start: (controller_) => {
        controller = controller_
      },
      cancel: async () => {
        done = true
        await cleanup()
      },
    })

    try {
      await this.ns.addWaiter(channel, notify)
      opts?.signal?.addEventListener('abort', abort, { once: true })
      if (opts?.signal?.aborted) abort()
      else {
        const result = await this.redis.get(resultKey)
        await this.ns.flushWaiters()
        await processing
        if (!done && result !== null) await emit(result)
        else if (!done && !opts?.allowMissing) {
          const state = await this.redis.hget(`${this.prefix}:${this.id}:j:${jobId}`, 'state')
          if (state === null || state === 'failed') {
            const lateResult = await this.redis.get(resultKey)
            await this.ns.flushWaiters()
            await processing
            if (!done && lateResult !== null) await emit(lateResult)
            else if (!done) fail(new ResultExpiredError(jobId))
          }
        }
      }
    } catch (err) {
      fail(err)
    }
    return events
  }

  async publish(
    jobId: string,
    event: Extract<QueueEvent, { type: 'started' | 'progress' }>,
  ): Promise<void> {
    await this.redis.publish(`${this.prefix}:${this.id}:done:${jobId}`, JSON.stringify(event))
  }

  async setStepData(jobId: string, stepName: string, value: string): Promise<void> {
    await this.redis.hset(`${this.prefix}:${this.id}:j:${jobId}:steps`, stepName, value)
  }

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

  async removeSchedule(scheduleId: string): Promise<void> {
    const scheduleKey = `${this.prefix}:${this.id}:schedule:${scheduleId}`
    const dueKey = `${this.prefix}:${this.id}:schedules:due`
    await this.redis.multi().del(scheduleKey).zrem(dueKey, scheduleId).exec()
  }

  async getSchedules(): Promise<ScheduleInfo[]> {
    const dueKey = `${this.prefix}:${this.id}:schedules:due`
    const entries = await this.redis.zrange(dueKey, 0, '-1', 'WITHSCORES')
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

  async getMetrics(): Promise<QueueMetrics> {
    const wf = `${this.prefix}:${this.id}`
    const [active, delayed, failed, groups] = await Promise.all([
      this.redis.zcard(`${wf}:active`),
      this.redis.zcard(`${wf}:delayed`),
      this.redis.zcard(`${wf}:failed`),
      this.redis.smembers(`${wf}:groups`),
    ])
    let waiting = 0
    if (groups.length > 0) {
      const pipeline = this.redis.pipeline()
      for (const groupId of groups) pipeline.zcard(`${wf}:g:${groupId}:jobs`)
      const results = (await pipeline.exec()) ?? []
      for (const [, count] of results) waiting += Number(count ?? 0)
    }
    return { active, waiting, delayed, failed }
  }

  async getGroupJobs(groupId: string) {
    const jobs = await this.redis.groupJobs(this.prefix, this.id, groupId)
    return jobs.map(([id, state, data, createdAt]) => ({
      id,
      state,
      data,
      createdAt: Number(createdAt),
    }))
  }

  async close(): Promise<void> {
    await Promise.all([...this.workers].map(async (w) => w.close()))
    this.workers.clear()
  }
}
