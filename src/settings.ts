import type { Meter } from '@opentelemetry/api'
import type { RedisOptions } from 'ioredis'
import { createSingletonPromise } from '@antfu/utils'
import IORedis from 'ioredis'

export type WorkflowLogger = {
  info?: (...data: any[]) => void
  success?: (...data: any[]) => void
  error?: (...data: any[]) => void
  debug?: (...data: any[]) => void
}

export const Settings = {
  defaultConnection: undefined as (() => Promise<IORedis> | IORedis) | undefined,
  logger: undefined as WorkflowLogger | undefined,
  metrics: undefined as { meter: Meter; prefix: string } | undefined,
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

export async function createRedis(opts: RedisOptions) {
  const redis = new IORedis({
    ...defaultRedisOptions,
    ...opts,
  })
  await redis.connect()
  return redis
}
