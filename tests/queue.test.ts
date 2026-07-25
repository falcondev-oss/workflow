import type Redis from 'ioredis'
import { randomUUID } from 'node:crypto'
import { beforeAll, expect, test, vi } from 'vitest'
import { createRedis } from '../src'
import { JobAlreadyExistsError, Namespace } from '../src/queue'

let redis: Redis
beforeAll(async () => {
  redis = await createRedis({
    host: 'localhost',
    port: Number(process.env.REDIS_PORT),
  })
})

async function connect() {
  return createRedis({ host: 'localhost', port: Number(process.env.REDIS_PORT) })
}

/** Cardinality of every active structure under a prefix — must be 0 after a full drain. */
async function activeCounts(prefix: string, wfId: string, nsId: string) {
  const groupActiveKeys = await redis.keys(`${prefix}:${wfId}:g:*:active`)
  let groupActive = 0
  for (const key of groupActiveKeys) groupActive += await redis.scard(key)
  const jobKeys = await redis.keys(`${prefix}:${wfId}:j:*`)
  return {
    wfActive: await redis.zcard(`${prefix}:${wfId}:active`),
    nsActive: await redis.scard(`${prefix}:ns:${nsId}:active`),
    ready: await redis.zcard(`${prefix}:${wfId}:ready`),
    groupActive,
    jobKeys: jobKeys.length,
  }
}

test('runs a job and wait() returns its result; no active structure leaks after drain', async () => {
  const prefix = randomUUID()
  const nsId = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })

  const seen = vi.fn()
  queue.worker((job) => {
    seen(job.data)
    return `result:${job.data}`
  })

  try {
    const { id } = await queue.add('hello')
    const result = await queue.wait(id)

    expect(result).toBe('result:hello')
    expect(seen).toHaveBeenCalledExactlyOnceWith('hello')

    await vi.waitFor(async () => {
      expect(await activeCounts(prefix, wfId, nsId)).toEqual({
        wfActive: 0,
        nsActive: 0,
        ready: 0,
        groupActive: 0,
        jobKeys: 0,
      })
    })
  } finally {
    await ns.close()
  }
})

test('wait() fans out to multiple concurrent waiters', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() })
  queue.worker(async () => {
    await new Promise((r) => setTimeout(r, 50))
    return 'done'
  })

  try {
    const { id } = await queue.add('payload')
    const results = await Promise.all([queue.wait(id), queue.wait(id), queue.wait(id)])
    expect(results).toEqual(['done', 'done', 'done'])
  } finally {
    await ns.close()
  }
})

test('add() throws JobAlreadyExistsError on a live id collision', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() })

  try {
    const jobId = randomUUID()
    await queue.add('first', { jobId })
    await expect(queue.add('second', { jobId })).rejects.toBeInstanceOf(JobAlreadyExistsError)
  } finally {
    await ns.close()
  }
})
