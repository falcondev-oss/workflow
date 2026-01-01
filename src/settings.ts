import { createSingletonPromise } from '@antfu/utils'
import IORedis from 'ioredis'

export const Settings = {
  defaultPrefix: 'falcondev-oss-workflow',
  defaultConnection: undefined as IORedis | undefined,
}

export const defaultRedisConnection = createSingletonPromise(async () => {
  if (Settings.defaultConnection) return Settings.defaultConnection

  const redis = new IORedis({ lazyConnect: true, maxRetriesPerRequest: null })
  await redis.connect()
  return redis
})
