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

type ReserveResult =
  | { kind: 'job'; claim: Claim }
  | { kind: 'empty'; msToNext: number }
  | { kind: 'maxed' }

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
  private readonly promoteBatchSize: number
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
    this.promoteBatchSize = opts?.promoteBatchSize ?? 500
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
      // Drain: reserve until a slot is unavailable or there is no runnable work. `msToNext`
      // is the reserve-reported ms until the next delayed job is due, carried out to the block.
      let msToNext = -1
      while (!this.closing && this.inFlight < this.concurrency) {
        const res = await this.reserve()
        if (res.kind === 'job') {
          this.start(res.claim)
          continue
        }
        if (res.kind === 'empty') msToNext = res.msToNext
        break
      }
      if (this.closing) break
      // Due work remained past the promote cap — re-reserve at once to drain the next chunk.
      if (msToNext === 0) continue
      // Block until woken (new work / freed slot / enqueued delay), the next delayed job
      // comes due, or the safety re-poll fires — whichever is sooner. The block is the timer.
      const timeout =
        msToNext < 0 ? this.safetyTimeout : Math.min(msToNext / 1000, this.safetyTimeout)
      await this.blockingRedis.brpop(this.wfWake, this.nsWake, timeout)
    }
  }

  private async reserve(): Promise<ReserveResult> {
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
      this.promoteBatchSize,
    )
    if (res[0] === 'maxed') return { kind: 'maxed' }
    if (res[0] === 'empty') return { kind: 'empty', msToNext: Number(res[1]) }
    const [, id, groupId, data, attempts, priority] = res as string[]
    const job: ReservedJob = Object.freeze({
      id: id!,
      groupId: groupId!,
      data: data!,
      attemptsMade: Number(attempts),
      priority: Number(priority),
    })
    return { kind: 'job', claim: { job, token } }
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
