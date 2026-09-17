/* eslint-disable no-console */
// PROTOTYPE, throwaway: how late does a BRPOP timeout fire?
import { RedisContainer } from '@testcontainers/redis'
import { createRedis } from '../../src'
for (const hz of [10, 100]) {
  const c = await new RedisContainer('redis:7-alpine')
    .withCommand(['redis-server', '--hz', String(hz)])
    .start()
  const r = await createRedis({ host: c.getHost(), port: c.getPort() })
  const late: number[] = []
  for (let i = 0; i < 40; i++) {
    const want = 20 + ((i * 37) % 300)
    const t = performance.now()
    await r.brpop('nothing', want / 1000)
    late.push(performance.now() - t - want)
  }
  late.sort((a, b) => a - b)
  console.log(
    `hz=${hz} lateness ms: min=${late[0]!.toFixed(0)} p50=${late[20]!.toFixed(0)} max=${late.at(-1)!.toFixed(0)}`,
  )
  r.disconnect()
  await c.stop()
}
