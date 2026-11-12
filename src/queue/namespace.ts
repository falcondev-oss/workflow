import type { Meter } from '@opentelemetry/api'
import type Redis from 'ioredis'
import type { QueueRedis } from './scripts'
import type {
  NamespaceOptions,
  QueueOptions,
  RateLimiterBudget,
  RateLimiterMetrics,
  WorkflowLogger,
} from './types'
import { Queue } from './queue'
import { registerScripts, UNLIMITED } from './scripts'

export class Namespace {
  private readonly subscriber: Redis
  private readonly subscriberReady: Promise<unknown>
  private readonly channelListeners = new Map<string, Set<(message: string) => void>>()
  private readonly channelSubscriptions = new Map<string, Promise<unknown>>()
  private readonly metricsMeters = new WeakSet<Meter>()
  private readonly queues = new Set<Queue>()

  readonly id: string
  readonly prefix: string
  readonly concurrency: number
  readonly redis: QueueRedis
  readonly logger?: WorkflowLogger
  readonly rateLimiters: Record<string, RateLimiterBudget>

  constructor(opts: NamespaceOptions) {
    this.id = opts.id
    this.prefix = opts.prefix ?? 'wf'
    this.concurrency = opts.concurrency ?? UNLIMITED
    this.logger = opts.logger
    this.rateLimiters = opts.rateLimiters ?? {}
    this.redis = registerScripts(opts.redis)

    this.subscriber = opts.redis.duplicate()
    this.subscriberReady = this.subscriber.connect().catch(() => {})
    this.subscriber.on('message', (channel: string, message: string) => {
      const listeners = this.channelListeners.get(channel) ?? []
      for (const cb of listeners) cb(message)
    })
  }

  queue(opts: QueueOptions): Queue {
    const queue = new Queue(this, opts)
    this.queues.add(queue)
    return queue
  }

  async getRateLimiterMetrics(): Promise<Record<string, RateLimiterMetrics>> {
    const values = await this.redis.rateLimiterMetrics(
      this.prefix,
      this.id,
      ...Object.keys(this.rateLimiters),
    )
    return Object.fromEntries(
      values.map(([name, starts, pausedMs]) => [name, { starts, pausedMs }]),
    )
  }

  setupRateLimiterMetrics({ meter, prefix }: { meter: Meter; prefix: string }): void {
    if (this.metricsMeters.has(meter)) return
    this.metricsMeters.add(meter)
    const starts = meter.createObservableGauge(`${prefix}_rate_limiter_starts`, {
      description: 'Unexpired job starts per rate limiter',
    })
    const paused = meter.createObservableGauge(`${prefix}_rate_limiter_paused_ms`, {
      description: 'Remaining rate limiter pause',
      unit: 'ms',
    })
    meter.addBatchObservableCallback(
      async (result) => {
        try {
          const metrics = await this.getRateLimiterMetrics()
          for (const [name, values] of Object.entries(metrics)) {
            const attributes = { rate_limiter: name }
            result.observe(starts, values.starts, attributes)
            result.observe(paused, values.pausedMs, attributes)
          }
        } catch (err) {
          this.logger?.error?.('Error collecting rate limiter metrics:', err)
        }
      },
      [starts, paused],
    )
  }

  async addWaiter(channel: string, cb: (message: string) => void): Promise<void> {
    await this.subscriberReady
    let set = this.channelListeners.get(channel)
    if (!set) {
      set = new Set()
      this.channelListeners.set(channel, set)
      this.channelSubscriptions.set(channel, this.subscriber.subscribe(channel))
    }
    set.add(cb)
    await this.channelSubscriptions.get(channel)
  }

  async removeWaiter(channel: string, cb: (message: string) => void): Promise<void> {
    const set = this.channelListeners.get(channel)
    if (!set) return
    set.delete(cb)
    if (set.size === 0) {
      await this.channelSubscriptions.get(channel)
      if (set.size > 0) return
      this.channelListeners.delete(channel)
      this.channelSubscriptions.delete(channel)
      await this.subscriber.unsubscribe(channel)
    }
  }

  async flushWaiters(): Promise<void> {
    await this.subscriber.ping()
  }

  async close(): Promise<void> {
    await Promise.all([...this.queues].map(async (q) => q.close()))
    this.subscriber.disconnect()
    this.redis.disconnect()
  }
}
