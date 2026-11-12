import type { Socket } from 'node:net'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { connect, createServer } from 'node:net'
import { Redis } from 'ioredis'
import { expect, onTestFinished, test, vi } from 'vitest'
import { WorkflowNamespace } from '../src'

async function redisProxy() {
  const proxy = { down: true }
  const sockets = new Set<Socket>()
  const server = createServer((client) => {
    if (proxy.down) return client.destroy()
    const upstream = connect(Number(process.env.REDIS_PORT), 'localhost')
    sockets.add(client).add(upstream)
    client.pipe(upstream).pipe(client)
    client.on('error', () => upstream.destroy())
    upstream.on('error', () => client.destroy())
  })
  server.listen(0)
  await once(server, 'listening')
  onTestFinished(() => {
    for (const socket of sockets) socket.destroy()
    server.close()
  })
  const address = server.address()
  if (typeof address !== 'object' || !address) throw new Error('proxy has no port')
  return Object.assign(proxy, { port: address.port })
}

test('a worker started while Redis is down keeps running and works once Redis is back', async () => {
  const proxy = await redisProxy()
  const redis = new Redis({
    port: proxy.port,
    lazyConnect: true,
    maxRetriesPerRequest: null,
    retryStrategy: () => 50,
  })
  onTestFinished(() => redis.disconnect())
  const workflow = new WorkflowNamespace({
    id: randomUUID(),
    redis,
    autoClose: false,
  }).createWorkflow({
    id: randomUUID(),
    run: async () => 'done',
  })

  const onError = vi.fn()
  const worker = await workflow.work({ onError })
  onTestFinished(async () => worker.close())

  await vi.waitFor(() => expect(onError).toHaveBeenCalled())

  proxy.down = false
  const job = await workflow.run(undefined)
  await expect(job.wait(10_000)).resolves.toBe('done')
})
