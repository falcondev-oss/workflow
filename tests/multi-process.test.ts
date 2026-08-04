/**
 * The acceptance criterion the in-process tests structurally cannot cover: several workers in
 * separate OS processes* against one Redis.
 *
 * This matters specifically for the perf rework, which moved three decisions out of Redis and
 * into each worker's own heap — the batch claim, the local slot park, and the local
 * stalled-scan throttle. Every one of those is invisible to peers by construction, so only real
 * processes can show that the Redis-side invariants (exactly-once, the caps, recovery of a
 * killed* process's claims) still hold between them.
 */

import type { Redis } from 'ioredis'
import type { ChildProcess } from 'node:child_process'
import { fork } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import process from 'node:process'
import { fileURLToPath } from 'node:url'
import { afterEach, beforeAll, expect, test } from 'vitest'
import { createRedis, WorkflowNamespace } from '../src'

const FIXTURE = fileURLToPath(new URL('fixtures/worker-process.ts', import.meta.url))

let redis: Redis
beforeAll(async () => {
  redis = await createRedis({ host: 'localhost', port: Number(process.env.REDIS_PORT) })
})

const children: ChildProcess[] = []
afterEach(() => {
  for (const child of children.splice(0)) child.kill('SIGKILL')
})

/** Fork a worker process and resolve once it has started draining. */
async function spawnWorker(opts: {
  prefix: string
  nsId: string
  wfId: string
  concurrency: number
}) {
  const child = fork(
    FIXTURE,
    [String(process.env.REDIS_PORT), opts.prefix, opts.nsId, opts.wfId, String(opts.concurrency)],
    { execArgv: ['--import', 'tsx'], stdio: ['ignore', 'inherit', 'inherit', 'ipc'] },
  )
  children.push(child)
  await new Promise<void>((resolve, reject) => {
    child.once('message', () => resolve())
    child.once('error', reject)
    child.once('exit', (code) => reject(new Error(`worker exited early with code ${code}`)))
  })
  return child
}

test('four worker processes drain a backlog exactly once each', async () => {
  const prefix = randomUUID()
  const nsId = randomUUID()
  const wfId = randomUUID()
  const JOBS = 120

  const ns = new WorkflowNamespace({
    id: nsId,
    prefix,
    redis: await createRedis({ host: 'localhost', port: Number(process.env.REDIS_PORT) }),
    autoClose: false,
  })
  const workflow = ns.createWorkflow({ id: wfId, run: async () => 'ok' })

  try {
    // Enqueue the whole backlog first, so every worker starts against a full queue and they all
    // race to batch-claim from it — the case where a greedy batch could double-claim.
    const jobs = await Promise.all(
      Array.from({ length: JOBS }, async () => workflow.run(undefined)),
    )

    await Promise.all(
      Array.from({ length: 4 }, async () => spawnWorker({ prefix, nsId, wfId, concurrency: 8 })),
    )

    await Promise.all(jobs.map(async (job) => job.wait(60_000)))

    // Exactly-once across processes: one increment per job, no more.
    expect(await redis.get(`${prefix}:runs`)).toBe(String(JOBS))
    expect(await redis.zcard(`${prefix}:${wfId}:active`)).toBe(0)
    expect(await redis.scard(`${prefix}:ns:${nsId}:active`)).toBe(0)
  } finally {
    await ns.close()
  }
}, 90_000)
