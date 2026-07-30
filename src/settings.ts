import type { RedisOptions } from 'ioredis'
import { createSingletonPromise } from '@antfu/utils'
import IORedis from 'ioredis'

const defaultRedisOptions: RedisOptions = {
  lazyConnect: true,
  maxRetriesPerRequest: null,
  retryStrategy: (times) => Math.max(Math.min(Math.exp(times), 20_000), 1000),
  enableOfflineQueue: false,
}

/** Fallback connection for namespaces created without an explicit `redis`, shared process-wide. */
export const defaultRedisConnection = createSingletonPromise(async () => {
  const redis = new IORedis(defaultRedisOptions)
  await redis.connect()
  return redis
})

export async function createRedis(opts: RedisOptions & { url?: string }) {
  const redisOpts = {
    ...defaultRedisOptions,
    ...opts,
  }
  const redis = opts.url ? new IORedis(opts.url, redisOpts) : new IORedis(redisOpts)
  await redis.connect()
  return redis
}
