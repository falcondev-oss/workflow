import type { RedisOptions } from 'ioredis'
import { createSingletonPromise } from '@antfu/utils'
import IORedis from 'ioredis'

export type WorkflowLogger = {
  info?: (...data: any[]) => void
  success?: (...data: any[]) => void
}

export const Settings = {
  defaultPrefix: 'falcondev-oss-workflow',
  defaultConnection: undefined as (() => Promise<IORedis> | IORedis) | undefined,
  defaultCronTimezone: undefined as string | undefined,
  logger: undefined as WorkflowLogger | undefined,
}

const defaultRedisOptions: RedisOptions = {
  lazyConnect: true,
  maxRetriesPerRequest: null,
  retryStrategy: (times) => Math.max(Math.min(Math.exp(times), 20_000), 1000),
  enableOfflineQueue: false,
}

export const defaultRedisConnection = createSingletonPromise(async () => {
  if (Settings.defaultConnection) return Settings.defaultConnection()

  const redis = new IORedis(defaultRedisOptions)
  await redis.connect()
  return redis
})

export async function createRedisConnection(opts: RedisOptions) {
  const redis = new IORedis({
    ...defaultRedisOptions,
    ...opts,
  })
  await redis.connect()
  return redis
}
