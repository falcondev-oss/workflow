import type Redis from 'ioredis'
import { randomUUID } from 'node:crypto'
import { sleep } from '@antfu/utils'
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

// ---------------------------------------------------------------------------
// Four-level concurrency caps (ticket 03)
// ---------------------------------------------------------------------------

/** Tracks live count + high-water mark; enter on job start, exit on job end. */
function makeTracker() {
  let current = 0
  let max = 0
  return {
    enter() {
      current++
      if (current > max) max = current
    },
    exit() {
      current--
    },
    get current() {
      return current
    },
    get max() {
      return max
    },
  }
}

/** A latch every gated handler awaits, so jobs pile up "running" until released. */
function makeGate() {
  let open!: () => void
  const promise = new Promise<void>((resolve) => {
    open = resolve
  })
  return { wait: async () => promise, open }
}

/** Wait until `cap` jobs are running, then prove no extra sneaks in past the cap. */
async function assertRunsExactly(tracker: ReturnType<typeof makeTracker>, cap: number) {
  await vi.waitFor(() => {
    expect(tracker.current).toBe(cap)
  })
  await sleep(100)
  expect(tracker.current).toBe(cap)
  expect(tracker.max).toBe(cap)
}

/** Assert every active structure for each workflow (and the shared namespace) is empty. */
async function assertDrained(prefix: string, wfIds: string[], nsId: string) {
  await vi.waitFor(async () => {
    expect(await redis.scard(`${prefix}:ns:${nsId}:active`)).toBe(0)
    for (const wfId of wfIds) {
      const groupActiveKeys = await redis.keys(`${prefix}:${wfId}:g:*:active`)
      let groupActive = 0
      for (const key of groupActiveKeys) groupActive += await redis.scard(key)
      const jobKeys = await redis.keys(`${prefix}:${wfId}:j:*`)
      expect({
        wfActive: await redis.zcard(`${prefix}:${wfId}:active`),
        ready: await redis.zcard(`${prefix}:${wfId}:ready`),
        groupActive,
        jobKeys: jobKeys.length,
      }).toEqual({ wfActive: 0, ready: 0, groupActive: 0, jobKeys: 0 })
    }
  })
}

test('worker cap: only `concurrency` jobs run in a process while the rest wait', async () => {
  const prefix = randomUUID()
  const nsId = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
  // namespace + workflow unlimited; every add its own group (group cap 1 never binds).
  const queue = ns.queue({ id: wfId })
  const tracker = makeTracker()
  const gate = makeGate()

  queue.worker(
    async () => {
      tracker.enter()
      await gate.wait()
      tracker.exit()
      return 'ok'
    },
    { concurrency: 2 },
  )

  try {
    for (let i = 0; i < 5; i++) await queue.add(`j${i}`)
    await assertRunsExactly(tracker, 2)
    gate.open()
    await assertDrained(prefix, [wfId], nsId)
  } finally {
    await ns.close()
  }
})

test('workflow cap: `Queue.concurrency` bounds a workflow even with worker headroom', async () => {
  const prefix = randomUUID()
  const nsId = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId, concurrency: 2 })
  const tracker = makeTracker()
  const gate = makeGate()

  queue.worker(
    async () => {
      tracker.enter()
      await gate.wait()
      tracker.exit()
      return 'ok'
    },
    { concurrency: 10 }, // worker room >> workflow cap, so the workflow cap is what binds
  )

  try {
    for (let i = 0; i < 5; i++) await queue.add(`j${i}`)
    await assertRunsExactly(tracker, 2)
    gate.open()
    await assertDrained(prefix, [wfId], nsId)
  } finally {
    await ns.close()
  }
})

test('namespace cap: `Namespace.concurrency` bounds all sibling workflows together', async () => {
  const prefix = randomUUID()
  const nsId = randomUUID()
  const wfA = randomUUID()
  const wfB = randomUUID()
  const ns = new Namespace({ id: nsId, concurrency: 2, redis: await connect(), prefix })
  const qA = ns.queue({ id: wfA }) // each workflow unlimited
  const qB = ns.queue({ id: wfB })
  const tracker = makeTracker()
  const gate = makeGate()

  const handler = async () => {
    tracker.enter()
    await gate.wait()
    tracker.exit()
    return 'ok'
  }
  qA.worker(handler, { concurrency: 10 })
  qB.worker(handler, { concurrency: 10 })

  try {
    for (let i = 0; i < 3; i++) await qA.add(`a${i}`)
    for (let i = 0; i < 3; i++) await qB.add(`b${i}`)
    await assertRunsExactly(tracker, 2) // 2 total across BOTH workflows
    gate.open()
    await assertDrained(prefix, [wfA, wfB], nsId)
  } finally {
    await ns.close()
  }
})

test('group cap 1 serializes a group; cap > 1 parallelizes it', async () => {
  // cap 1
  {
    const prefix = randomUUID()
    const nsId = randomUUID()
    const wfId = randomUUID()
    const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
    const queue = ns.queue({ id: wfId, groupConcurrency: 1 })
    const tracker = makeTracker()
    const gate = makeGate()
    queue.worker(
      async () => {
        tracker.enter()
        await gate.wait()
        tracker.exit()
        return 'ok'
      },
      { concurrency: 10 },
    )
    try {
      for (let i = 0; i < 3; i++) await queue.add(`g${i}`, { groupId: 'grp' })
      await assertRunsExactly(tracker, 1) // serialized
      gate.open()
      await assertDrained(prefix, [wfId], nsId)
    } finally {
      await ns.close()
    }
  }

  // cap 3
  {
    const prefix = randomUUID()
    const nsId = randomUUID()
    const wfId = randomUUID()
    const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
    const queue = ns.queue({ id: wfId, groupConcurrency: 3 })
    const tracker = makeTracker()
    const gate = makeGate()
    queue.worker(
      async () => {
        tracker.enter()
        await gate.wait()
        tracker.exit()
        return 'ok'
      },
      { concurrency: 10 },
    )
    try {
      for (let i = 0; i < 5; i++) await queue.add(`g${i}`, { groupId: 'grp' })
      await assertRunsExactly(tracker, 3) // 3 of one group run at once
      gate.open()
      await assertDrained(prefix, [wfId], nsId)
    } finally {
      await ns.close()
    }
  }
})

test('combined gating: a top-gate binds even when group and worker have room', async () => {
  const prefix = randomUUID()
  const nsId = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
  // workflow cap 2 is the only thing that should bind: group cap 5 and worker cap 5 both
  // have plenty of room, yet no more than 2 may run.
  const queue = ns.queue({ id: wfId, concurrency: 2, groupConcurrency: 5 })
  const tracker = makeTracker()
  const gate = makeGate()
  queue.worker(
    async () => {
      tracker.enter()
      await gate.wait()
      tracker.exit()
      return 'ok'
    },
    { concurrency: 5 },
  )

  try {
    // 4 jobs across 2 groups (2 each): groups could run 2 apiece, worker could run 5.
    for (let i = 0; i < 2; i++) await queue.add(`x${i}`, { groupId: 'A' })
    for (let i = 0; i < 2; i++) await queue.add(`y${i}`, { groupId: 'B' })
    await assertRunsExactly(tracker, 2)
    gate.open()
    await assertDrained(prefix, [wfId], nsId)
  } finally {
    await ns.close()
  }
})

test('a capped group does not block a runnable sibling group (no head-of-line stall)', async () => {
  const prefix = randomUUID()
  const nsId = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId, groupConcurrency: 1 })
  const tracker = makeTracker()
  const gate = makeGate()
  queue.worker(
    async () => {
      tracker.enter()
      await gate.wait()
      tracker.exit()
      return 'ok'
    },
    { concurrency: 10 },
  )

  try {
    // Group A is enqueued first with 3 jobs (cap 1 ⇒ 2 of them are stuck behind the head).
    for (let i = 0; i < 3; i++) await queue.add(`a${i}`, { groupId: 'A' })
    // Group B has a single job; it must run even though A's backlog sits ahead in time.
    await queue.add('b0', { groupId: 'B' })
    // One from A + one from B run concurrently; A's cap does not wedge B.
    await assertRunsExactly(tracker, 2)
    gate.open()
    await assertDrained(prefix, [wfId], nsId)
  } finally {
    await ns.close()
  }
})

test('contention invariant: K workers × M jobs, no leak, no cap ever exceeded', async () => {
  const prefix = randomUUID()
  const nsId = randomUUID()
  const wfA = randomUUID()
  const wfB = randomUUID()
  const NS_CAP = 3
  const WF_CAP = 2
  const GROUP_CAP = 2
  const WORKER_CAP = 2

  const ns = new Namespace({ id: nsId, concurrency: NS_CAP, redis: await connect(), prefix })
  const queues = {
    [wfA]: ns.queue({ id: wfA, concurrency: WF_CAP, groupConcurrency: GROUP_CAP }),
    [wfB]: ns.queue({ id: wfB, concurrency: WF_CAP, groupConcurrency: GROUP_CAP }),
  }

  const nsTracker = makeTracker()
  const wfTrackers: Record<string, ReturnType<typeof makeTracker>> = {
    [wfA]: makeTracker(),
    [wfB]: makeTracker(),
  }
  const groupTrackers = new Map<string, ReturnType<typeof makeTracker>>()
  const workerTrackers: ReturnType<typeof makeTracker>[] = []
  const groupTracker = (key: string) => {
    let t = groupTrackers.get(key)
    if (!t) {
      t = makeTracker()
      groupTrackers.set(key, t)
    }
    return t
  }

  try {
    // 2 workers per workflow (K = 4 total), each worker cap 2.
    for (const wfId of [wfA, wfB]) {
      for (let w = 0; w < 2; w++) {
        const workerTracker = makeTracker()
        workerTrackers.push(workerTracker)
        queues[wfId]!.worker(
          async (job) => {
            const gt = groupTracker(`${wfId}:${job.groupId}`)
            nsTracker.enter()
            wfTrackers[wfId]!.enter()
            gt.enter()
            workerTracker.enter()
            await sleep(5 + Math.floor(Math.random() * 15))
            workerTracker.exit()
            gt.exit()
            wfTrackers[wfId]!.exit()
            nsTracker.exit()
            return 'ok'
          },
          { concurrency: WORKER_CAP },
        )
      }
    }

    // M jobs per workflow: 3 groups × 2 jobs each = 6, so 12 jobs total.
    for (const wfId of [wfA, wfB]) {
      for (let g = 0; g < 3; g++) {
        for (let j = 0; j < 2; j++) {
          await queues[wfId]!.add(`${wfId}-g${g}-j${j}`, { groupId: `${wfId}-g${g}` })
        }
      }
    }

    await assertDrained(prefix, [wfA, wfB], nsId)

    // No cap was ever exceeded at any level.
    expect(nsTracker.max).toBeLessThanOrEqual(NS_CAP)
    for (const wfId of [wfA, wfB]) expect(wfTrackers[wfId]!.max).toBeLessThanOrEqual(WF_CAP)
    for (const t of groupTrackers.values()) expect(t.max).toBeLessThanOrEqual(GROUP_CAP)
    for (const t of workerTrackers) expect(t.max).toBeLessThanOrEqual(WORKER_CAP)
    // Sanity: work actually overlapped somewhere (otherwise the caps proved nothing).
    expect(nsTracker.max).toBeGreaterThan(1)
  } finally {
    await ns.close()
  }
})

// ---------------------------------------------------------------------------
// Numeric priority ordering (ticket 04)
// ---------------------------------------------------------------------------

// Ordering is observable only with a single worker at concurrency 1: it pops one job at
// a time, so the sequence it sees is exactly the queue's chosen order. All jobs are added
// *before* the worker exists, so the full waiting set is present when it starts draining.

test('priority: higher runs first across groups; default 0 runs after any expedited job', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() }) // each add = its own group (default cap 1)

  try {
    // Enqueue in an order that does NOT match priority order, across distinct groups.
    await queue.add('p0-a') // default priority 0
    await queue.add('p5', { priority: 5 })
    await queue.add('p10', { priority: 10 })
    await queue.add('p0-b') // default priority 0

    const order: string[] = []
    queue.worker(
      (job) => {
        order.push(job.data)
        return 'ok'
      },
      { concurrency: 1 },
    )

    // Highest priority first; equal-priority (both 0) fall back to FIFO-by-enqueue.
    await vi.waitFor(() => {
      expect(order).toEqual(['p10', 'p5', 'p0-a', 'p0-b'])
    })
  } finally {
    await ns.close()
  }
})

test('priority: equal-priority jobs run FIFO by promotion (enqueue) order', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() })

  try {
    const added = ['a', 'b', 'c', 'd', 'e']
    for (const d of added) await queue.add(d, { priority: 7 }) // all equal priority

    const order: string[] = []
    queue.worker(
      (job) => {
        order.push(job.data)
        return 'ok'
      },
      { concurrency: 1 },
    )

    await vi.waitFor(() => {
      expect(order).toEqual(added) // tiebreak = FIFO by the counter stamped at promotion
    })
  } finally {
    await ns.close()
  }
})

test('add() rejects an out-of-range or non-integer priority', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() })

  try {
    await expect(queue.add('x', { priority: -1 })).rejects.toBeInstanceOf(RangeError)
    await expect(queue.add('x', { priority: 2 ** 21 })).rejects.toBeInstanceOf(RangeError)
    await expect(queue.add('x', { priority: 1.5 })).rejects.toBeInstanceOf(RangeError)
    // Boundaries are accepted.
    await queue.add('lo', { priority: 0 })
    await queue.add('hi', { priority: 2 ** 21 - 1 })
  } finally {
    await ns.close()
  }
})
