import type { StartedRedisContainer } from '@testcontainers/redis'
import { RedisContainer } from '@testcontainers/redis'

let redisContainer: StartedRedisContainer

export async function setup() {
  redisContainer = await new RedisContainer('redis:7-alpine').start()
  process.env.REDIS_PORT = redisContainer.getPort().toString()
}

export async function teardown() {
  await redisContainer.stop({
    remove: true,
    removeVolumes: true,
  })
}
