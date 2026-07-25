import type { Namespace } from './namespace'
import type { QueueRedis } from './scripts'
import type { AddOptions, QueueOptions, WaitOptions, WorkerOptions } from './types'
import { randomUUID } from 'node:crypto'
import { JobAlreadyExistsError, ResultExpiredError, TimeoutError } from './errors'
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

  /** Enqueue an immediate job. Throws `JobAlreadyExistsError` on a live id collision. */
  async add(data: string, opts?: AddOptions): Promise<{ id: string; groupId: string }> {
    const id = opts?.jobId ?? randomUUID()
    const groupId = opts?.groupId ?? randomUUID()
    const priority = opts?.priority ?? 0
    // Guard the range that keeps the packed score exact in a ZSET double (§6). An
    // out-of-range or fractional priority would corrupt score packing / ordering.
    if (!Number.isInteger(priority) || priority < 0 || priority > PMAX)
      throw new RangeError(`priority must be an integer in 0…${PMAX}, got ${priority}`)
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
    const record = JSON.parse(raw) as { state: string; value?: string; reason?: string }
    if (record.state === 'failed') throw new Error(record.reason ?? 'job failed')
    return record.value ?? ''
  }

  /** Releases queue-owned handles: closes workers and unsubscribes any `wait()` channels. */
  async close(): Promise<void> {
    await Promise.all([...this.workers].map(async (w) => w.close()))
    this.workers.clear()
  }
}
