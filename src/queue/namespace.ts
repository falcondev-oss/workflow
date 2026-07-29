import type Redis from 'ioredis'
import type { QueueRedis } from './scripts'
import type { NamespaceOptions, QueueOptions } from './types'
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

  private readonly subscriber: Redis
  private readonly subscriberReady: Promise<unknown>
  private readonly channelListeners = new Map<string, Set<() => void>>()
  private readonly queues = new Set<Queue>()

  constructor(opts: NamespaceOptions) {
    this.id = opts.id
    this.prefix = opts.prefix ?? 'wf'
    this.concurrency = opts.concurrency ?? UNLIMITED
    this.redis = registerScripts(opts.redis)

    this.subscriber = opts.redis.duplicate()
    this.subscriberReady = this.subscriber.connect().catch(() => {})
    this.subscriber.on('message', (channel: string) => {
      for (const cb of this.channelListeners.get(channel) ?? []) cb()
    })
  }

  queue(opts: QueueOptions): Queue {
    const queue = new Queue(this, opts)
    this.queues.add(queue)
    return queue
  }

  /** Register a waiter on a pub/sub channel; subscribes on first listener. */
  async addWaiter(channel: string, cb: () => void): Promise<void> {
    await this.subscriberReady
    let set = this.channelListeners.get(channel)
    if (!set) {
      set = new Set()
      this.channelListeners.set(channel, set)
      await this.subscriber.subscribe(channel)
    }
    set.add(cb)
  }

  /** Remove a waiter; unsubscribes once the channel has no listeners. */
  async removeWaiter(channel: string, cb: () => void): Promise<void> {
    const set = this.channelListeners.get(channel)
    if (!set) return
    set.delete(cb)
    if (set.size === 0) {
      this.channelListeners.delete(channel)
      await this.subscriber.unsubscribe(channel)
    }
  }

  /** Drains and closes every queue, then disconnects the shared + pub/sub connections. */
  async close(): Promise<void> {
    await Promise.all([...this.queues].map(async (q) => q.close()))
    this.subscriber.disconnect()
    this.redis.disconnect()
  }
}
