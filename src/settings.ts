import { createSingletonPromise } from '@antfu/utils'
import IORedis from 'ioredis'

export type WorkflowLogger = {
  info?: (...data: any[]) => void
  success?: (...data: any[]) => void
}

export const Settings = {
  defaultPrefix: 'falcondev-oss-workflow',
  defaultConnection: undefined as IORedis | undefined,
  defaultCronTimezone: undefined as string | undefined,
  logger: undefined as WorkflowLogger | undefined,
}

export const defaultRedisConnection = createSingletonPromise(async () => {
  if (Settings.defaultConnection) return Settings.defaultConnection

  const redis = new IORedis({ lazyConnect: true, maxRetriesPerRequest: null })
  await redis.connect()
  return redis
})
