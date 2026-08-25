import type Redis from 'ioredis'
import type { QueueRedis } from './scripts'
import type { NamespaceOptions, QueueOptions, WorkflowLogger } from './types'
import { Queue } from './queue'
import { registerScripts, UNLIMITED } from './scripts'

/**
 * Owns the shared redis connection, the single pub/sub connection, and the cross-workflow
 * concurrency cap. Mints queues and cascades `close`. The pub/sub connection backs every
 * `wait()`, so waiting works from a pure producer with no worker.
 */
export class Namespace {
  readonly id: string
  readonly prefix: string
  readonly concurrency: number
  readonly redis: QueueRedis
  readonly logger?: WorkflowLogger

  private readonly subscriber: Redis
  private readonly subscriberReady: Promise<unknown>
  private readonly channelListeners = new Map<string, Set<(message: string) => void>>()
  private readonly channelSubscriptions = new Map<string, Promise<unknown>>()
  private readonly queues = new Set<Queue>()

  constructor(opts: NamespaceOptions) {
    this.id = opts.id
    this.prefix = opts.prefix ?? 'wf'
    this.concurrency = opts.concurrency ?? UNLIMITED
    this.logger = opts.logger
    this.redis = registerScripts(opts.redis)

    this.subscriber = opts.redis.duplicate()
    this.subscriberReady = this.subscriber.connect().catch(() => {})
    this.subscriber.on('message', (channel: string, message: string) => {
      for (const cb of this.channelListeners.get(channel) ?? []) cb(message)
    })
  }

  queue(opts: QueueOptions): Queue {
    const queue = new Queue(this, opts)
    this.queues.add(queue)
    return queue
  }

  /** Register a waiter on a pub/sub channel; subscribes on first listener. */
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

  /** Remove a waiter; unsubscribes once the channel has no listeners. */
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

  /** Wait until this connection has dispatched every Pub/Sub message Redis sent before the ping. */
  async flushWaiters(): Promise<void> {
    await this.subscriber.ping()
  }

  /** Drains and closes every queue, then disconnects the shared + pub/sub connections. */
  async close(): Promise<void> {
    await Promise.all([...this.queues].map(async (q) => q.close()))
    this.subscriber.disconnect()
    this.redis.disconnect()
  }
}
