import type Redis from 'ioredis'
import type { Queue } from './queue'
import type { QueueRedis } from './scripts'
import type { JobContext, ReservedJob, WorkerOptions } from './types'
import { randomUUID } from 'node:crypto'
import { Settings } from '../settings'

export type WorkerHandler = (job: ReservedJob, ctx: JobContext) => Promise<string> | string

interface Claim {
  job: ReservedJob
  token: string
}

/**
 * Runs jobs for one workflow. A JS-local semaphore (`concurrency`) is the per-worker cap —
 * no Redis round-trip. The loop drains reserve until empty/maxed, then blocks on
 * `BRPOP wf:wake ns:wake <safetyTimeout>` (its own connection); enqueue and every release
 * `LPUSH` those wake lists, so a freed slot or new work re-drives the loop.
 */
export class Worker {
  private readonly concurrency: number
  private readonly lockMs: number
  private readonly safetyTimeout: number
  private readonly onError: (error: unknown) => void

  private readonly blockingRedis: Redis
  private readonly redis: QueueRedis
  private readonly wfWake: string
  private readonly nsWake: string

  private inFlight = 0
  private closing = false
  private readonly inProgress = new Set<Promise<void>>()
  private readonly loopPromise: Promise<void>

  constructor(
    private readonly queue: Queue,
    readonly handler: WorkerHandler,
    opts?: WorkerOptions,
  ) {
    this.concurrency = opts?.concurrency ?? 1
    this.lockMs = opts?.lockMs ?? 30_000
    this.safetyTimeout = opts?.safetyTimeout ?? 5
    this.onError = opts?.onError ?? ((error) => Settings.logger?.error?.(error))

    this.redis = queue.redis
    this.blockingRedis = queue.redis.duplicate()
    this.wfWake = `${queue.prefix}:${queue.id}:wake`
    this.nsWake = `${queue.prefix}:ns:${queue.ns.id}:wake`

    this.loopPromise = this.loop()
  }

  private async loop(): Promise<void> {
    if (this.blockingRedis.status === 'wait') await this.blockingRedis.connect()
    while (!this.closing) {
      // Drain: reserve until a slot is unavailable or there is no runnable work.
      while (!this.closing && this.inFlight < this.concurrency) {
        const claim = await this.reserve()
        if (!claim) break
        this.start(claim)
      }
      if (this.closing) break
      // Block until woken (new work / freed slot) or the safety re-poll fires.
      await this.blockingRedis.brpop(this.wfWake, this.nsWake, this.safetyTimeout)
    }
  }

  private async reserve(): Promise<Claim | undefined> {
    const token = randomUUID()
    const res = await this.redis.reserve(
      this.queue.prefix,
      this.queue.id,
      this.queue.ns.id,
      this.queue.ns.concurrency,
      this.queue.concurrency,
      this.queue.groupConcurrency,
      this.lockMs,
      token,
    )
    if (res[0] !== 'job') return undefined
    const [, id, groupId, data, attempts, priority] = res
    const job: ReservedJob = Object.freeze({
      id: id!,
      groupId: groupId!,
      data: data!,
      attemptsMade: Number(attempts),
      priority: Number(priority),
    })
    return { job, token }
  }

  private start(claim: Claim): void {
    this.inFlight++
    const promise = this.process(claim).finally(() => {
      this.inFlight--
      this.inProgress.delete(promise)
    })
    this.inProgress.add(promise)
  }

  private async process(claim: Claim): Promise<void> {
    const controller = new AbortController()
    try {
      const result = await this.handler(claim.job, { signal: controller.signal })
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
      // Retry/fail is a later ticket. Surface the error; the claim is released by
      // stalled-recovery (also later) once the lock TTL lapses.
      this.onError(err)
    }
  }

  /** Stop accepting work, drain in-flight jobs, then quit the blocking connection. */
  async close(): Promise<void> {
    this.closing = true
    // Unblock a pending BRPOP so the loop can observe `closing`.
    await this.redis.lpush(this.wfWake, '1')
    await this.redis.ltrim(this.wfWake, 0, 0)
    await this.loopPromise
    await Promise.all(this.inProgress)
    this.blockingRedis.disconnect()
  }
}
