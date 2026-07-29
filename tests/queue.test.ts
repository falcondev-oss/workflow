import type Redis from 'ioredis'
import type { ReservedJob } from '../src/queue'
import { randomUUID } from 'node:crypto'
import { sleep } from '@antfu/utils'
import { beforeAll, expect, test, vi } from 'vitest'
import { createRedis } from '../src'
import {
  expBackoff,
  JobAlreadyExistsError,
  Namespace,
  ResultExpiredError,
  TimeoutError,
} from '../src/queue'

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

// ---------------------------------------------------------------------------
// Mutable step data (ticket 05)
// ---------------------------------------------------------------------------

test('setStepData/getStepData round-trips an opaque string; null on miss', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() })

  try {
    const jobId = randomUUID()
    expect(await queue.getStepData(jobId, 'step-a')).toBeNull() // never written

    await queue.setStepData(jobId, 'step-a', 'opaque \n{"x":1}')
    expect(await queue.getStepData(jobId, 'step-a')).toBe('opaque \n{"x":1}')
    expect(await queue.getStepData(jobId, 'step-b')).toBeNull() // distinct field still missing
  } finally {
    await ns.close()
  }
})

test('parallel setStepData on distinct steps both persist (no lost update)', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() })

  try {
    const jobId = randomUUID()
    await Promise.all([
      queue.setStepData(jobId, 'a', 'value-a'),
      queue.setStepData(jobId, 'b', 'value-b'),
    ])
    expect(await queue.getStepData(jobId, 'a')).toBe('value-a')
    expect(await queue.getStepData(jobId, 'b')).toBe('value-b')
  } finally {
    await ns.close()
  }
})

test(':steps hash is deleted when the job completes', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })

  queue.worker(async (job) => {
    await queue.setStepData(job.id, 'step', 'checkpoint')
    return 'done'
  })

  try {
    const { id } = await queue.add('payload')
    await queue.wait(id)

    const stepsKey = `${prefix}:${wfId}:j:${id}:steps`
    await vi.waitFor(async () => {
      expect(await redis.exists(stepsKey)).toBe(0)
    })
  } finally {
    await ns.close()
  }
})

// ---------------------------------------------------------------------------
// Delayed / scheduled jobs (ticket 06)
// ---------------------------------------------------------------------------

test('runAt stores the absolute score; runIn resolves against the Redis clock', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId }) // no worker: delayed jobs just sit in the ZSET
  const delayedKey = `${prefix}:${wfId}:delayed`

  try {
    // Absolute runAt is stored verbatim as the score.
    const runAt = Date.now() + 3_600_000
    const { id: a } = await queue.add('at', { runAt })
    expect(Number(await redis.zscore(delayedKey, a))).toBe(runAt)

    // runIn resolves to `redisNow + runIn`; bracket it with generous skew tolerance.
    const before = Date.now()
    const { id: b } = await queue.add('in', { runIn: 3_600_000 })
    const after = Date.now()
    const score = Number(await redis.zscore(delayedKey, b))
    expect(score).toBeGreaterThanOrEqual(before + 3_600_000 - 2000)
    expect(score).toBeLessThanOrEqual(after + 3_600_000 + 2000)

    // Both are parked as delayed, neither entered the waiting structure.
    expect(await redis.zcard(delayedKey)).toBe(2)
    expect(await redis.zcard(`${prefix}:${wfId}:ready`)).toBe(0)
  } finally {
    await ns.close()
  }
})

test('runAt and runIn are mutually exclusive', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() })

  try {
    await expect(
      // @ts-expect-error `runAt` and `runIn` are mutually exclusive at the type level
      queue.add('x', { runAt: Date.now() + 1000, runIn: 1000 }),
    ).rejects.toThrow(/mutually exclusive/)
  } finally {
    await ns.close()
  }
})

test('a past runAt skips the delayed ZSET and enqueues straight into waiting', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId }) // no worker: observe the raw enqueue placement

  try {
    const { id, groupId } = await queue.add('past', { runAt: Date.now() - 1000 })
    // Never entered `delayed`; it is immediately runnable in its group's waiting ZSET.
    expect(await redis.zcard(`${prefix}:${wfId}:delayed`)).toBe(0)
    expect(await redis.zscore(`${prefix}:${wfId}:g:${groupId}:jobs`, id)).not.toBeNull()
    expect(await redis.zcard(`${prefix}:${wfId}:ready`)).toBe(1)
  } finally {
    await ns.close()
  }
})

test('promotion order is FIFO-by-due-time, not enqueue order', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId }) // each add its own group ⇒ pure priority/FIFO index
  const delayedKey = `${prefix}:${wfId}:delayed`

  try {
    // Enqueue far in the future so nothing auto-promotes, then back-date the stored scores
    // (no fake clock) so due order (y < z < x) differs from enqueue order (x, y, z).
    const ids: Record<string, string> = {}
    for (const d of ['x', 'y', 'z']) {
      const { id } = await queue.add(d, { runAt: Date.now() + 3_600_000 })
      ids[d] = id
    }
    const base = Date.now() - 10_000
    await redis.zadd(delayedKey, base + 3, ids.x!) // due last
    await redis.zadd(delayedKey, base + 1, ids.y!) // due first
    await redis.zadd(delayedKey, base + 2, ids.z!) // due second

    // Single worker at concurrency 1 observes exactly the promotion (= due) order.
    const order: string[] = []
    queue.worker(
      (job) => {
        order.push(job.data)
        return 'ok'
      },
      { concurrency: 1 },
    )

    await vi.waitFor(() => {
      expect(order).toEqual(['y', 'z', 'x'])
    })
  } finally {
    await ns.close()
  }
})

test('per-call promote cap drains a larger backlog in chunks', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })
  const delayedKey = `${prefix}:${wfId}:delayed`
  const N = 10

  try {
    // Enqueue future, then back-date every score so all N are due at once — more than the
    // promote cap, so a single reserve cannot promote them all.
    const base = Date.now() - 10_000
    for (let i = 0; i < N; i++) {
      const { id } = await queue.add(`d${i}`, { runAt: Date.now() + 3_600_000 })
      await redis.zadd(delayedKey, base + i, id)
    }

    const seen = new Set<string>()
    queue.worker(
      (job) => {
        seen.add(job.data)
        return 'ok'
      },
      { concurrency: 2, promoteBatchSize: 3 }, // cap 3 << 10 ⇒ must re-reserve to drain
    )

    // All eventually run (the worker re-reserves to promote the next chunk), and the
    // delayed ZSET fully empties.
    await vi.waitFor(() => {
      expect(seen.size).toBe(N)
    })
    expect(await redis.zcard(delayedKey)).toBe(0)
  } finally {
    await ns.close()
  }
})

test('an idle worker picks up a delayed job when it comes due (block is the timer)', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() })

  const seen = vi.fn()
  queue.worker((job) => {
    seen(job.data)
    return 'ok'
  })

  try {
    // Worker is idle-blocked; a short-delay job must be promoted and run without a poller.
    const { id } = await queue.add('soon', { runIn: 300 })
    const result = await queue.wait(id)
    expect(result).toBe('ok')
    expect(seen).toHaveBeenCalledExactlyOnceWith('soon')
  } finally {
    await ns.close()
  }
})

// ---------------------------------------------------------------------------
// Retries, backoff, dead-letter, retention & wait() contract (ticket 07)
// ---------------------------------------------------------------------------

test('expBackoff jitters within [0, ceiling] and caps the ceiling', () => {
  const b = expBackoff({ base: 100, factor: 2, cap: 1000 })
  for (let attempt = 1; attempt <= 8; attempt++) {
    const ceiling = Math.min(1000, 100 * 2 ** (attempt - 1))
    for (let i = 0; i < 50; i++) {
      const d = b(attempt)
      expect(d).toBeGreaterThanOrEqual(0)
      expect(d).toBeLessThanOrEqual(ceiling)
    }
  }
})

test('wait() throws the job failure, carrying its reason', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() })
  queue.worker(() => {
    throw new Error('boom')
  })

  try {
    const { id } = await queue.add('x')
    await expect(queue.wait(id)).rejects.toThrow('boom')
  } finally {
    await ns.close()
  }
})

test('wait() rejects with TimeoutError when the deadline elapses', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() }) // no worker ⇒ never finishes

  try {
    const { id } = await queue.add('x')
    await expect(queue.wait(id, { timeoutMs: 100 })).rejects.toBeInstanceOf(TimeoutError)
  } finally {
    await ns.close()
  }
})

test('wait() rejects with ResultExpiredError when notified but the result is gone', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId }) // no worker, so no result record is ever written

  try {
    const jobId = randomUUID()
    const waiting = queue.wait(jobId) // subscribes, reads null, then blocks on the publish
    // Race the subscribe registration: publish `done` (no result key) until the waiter observes it.
    const channel = `${prefix}:${wfId}:done:${jobId}`
    const pump = setInterval(() => void redis.publish(channel, '1'), 20)
    await expect(waiting).rejects.toBeInstanceOf(ResultExpiredError)
    clearInterval(pump)
  } finally {
    await ns.close()
  }
})

test('a retryable failure requeues via delayed, releasing all slots, then re-runs', async () => {
  const prefix = randomUUID()
  const nsId = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })
  const delayedKey = `${prefix}:${wfId}:delayed`
  const wfWake = `${prefix}:${wfId}:wake`

  const attempts: number[] = []
  queue.worker(
    (job) => {
      attempts.push(job.attemptsMade)
      if (job.attemptsMade === 0) throw new Error('transient')
      return 'ok'
    },
    // Long backoff so the delayed/slot-released window is observable; back-date to re-run.
    { backoff: () => 10_000, safetyTimeout: 0.2 },
  )

  try {
    const { id } = await queue.add('x', { maxAttempts: 2 })

    // The first failure requeues the job into `delayed` (not terminal).
    await vi.waitFor(async () => {
      expect(await redis.zscore(delayedKey, id)).not.toBeNull()
    })
    // During backoff every concurrency slot is freed — only the delayed parking remains.
    const counts = await activeCounts(prefix, wfId, nsId)
    expect(counts.wfActive).toBe(0)
    expect(counts.nsActive).toBe(0)
    expect(counts.groupActive).toBe(0)
    expect(counts.ready).toBe(0)
    expect(attempts).toEqual([0])

    // Make it due and kick the worker; it promotes, re-runs (attemptsMade now 1), and completes.
    await redis.zadd(delayedKey, Date.now() - 1000, id)
    await redis.lpush(wfWake, '1')
    expect(await queue.wait(id)).toBe('ok')
    expect(attempts).toEqual([0, 1])
  } finally {
    await ns.close()
  }
})

test('maxAttempts exhaustion dead-letters to the failed ZSET', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })
  queue.worker(() => {
    throw new Error('always')
  })

  try {
    const { id } = await queue.add('x') // maxAttempts default 1 ⇒ first failure is terminal
    await expect(queue.wait(id)).rejects.toThrow('always')

    expect(await redis.zscore(`${prefix}:${wfId}:failed`, id)).not.toBeNull()
    const hash = await redis.hgetall(`${prefix}:${wfId}:j:${id}`)
    expect(hash.state).toBe('failed')
    expect(hash.failedReason).toBe('always')
    expect(hash.attempts).toBe('1') // incremented in Lua, not JS
    expect(hash.stalledCount).toBe('0') // separate budget, untouched
    // The claim was released and step data cleared.
    expect(await redis.zcard(`${prefix}:${wfId}:active`)).toBe(0)
    expect(await redis.exists(`${prefix}:${wfId}:j:${id}:steps`)).toBe(0)
  } finally {
    await ns.close()
  }
})

test('keepFailed count-trims the failed ZSET and DELs evicted job hashes', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })
  queue.worker(
    () => {
      throw new Error('nope')
    },
    { concurrency: 1, keepFailed: 2 },
  )

  try {
    // Fail 4 jobs one at a time (sequenced via wait) ⇒ strictly increasing `finishedOn`.
    const ids: string[] = []
    for (let i = 0; i < 4; i++) {
      const { id } = await queue.add(`j${i}`)
      ids.push(id)
      await queue.wait(id).catch(() => {})
    }

    const failedKey = `${prefix}:${wfId}:failed`
    await vi.waitFor(async () => {
      expect(await redis.zcard(failedKey)).toBe(2) // only the newest 2 retained
    })
    // Oldest two evicted from the ZSET and their hashes DEL'd; newest two kept.
    expect(await redis.exists(`${prefix}:${wfId}:j:${ids[0]}`)).toBe(0)
    expect(await redis.exists(`${prefix}:${wfId}:j:${ids[1]}`)).toBe(0)
    expect(await redis.zscore(failedKey, ids[2]!)).not.toBeNull()
    expect(await redis.zscore(failedKey, ids[3]!)).not.toBeNull()
  } finally {
    await ns.close()
  }
})

// ---------------------------------------------------------------------------
// Stalled-job recovery, heartbeat & abort-on-lost-claim (ticket 08)
// ---------------------------------------------------------------------------

// No fake clock: a stalled worker is simulated by BACK-DATING the stored `wf:active` deadline
// score so the deadline-compare scan (`ZRANGEBYSCORE 0 now`) sees the claim as expired.

test('a stalled active job (deadline back-dated) is recovered and re-runs', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })
  const wfActive = `${prefix}:${wfId}:active`

  let calls = 0
  const started = makeGate()
  const gate = makeGate()
  queue.worker(
    async () => {
      calls++
      if (calls === 1) {
        started.open()
        await gate.wait() // first invocation "hangs" as if its process died
      }
      return `ok:${calls}`
    },
    { safetyTimeout: 0.2, stalledInterval: 100 }, // fast wake-loop re-poll + throttle
  )

  try {
    const { id } = await queue.add('x')
    await started.wait() // first invocation is running ⇒ state=active in wf:active

    // Deadline in the past ⇒ a candidate for the scan. The 10s heartbeat can't renew within
    // the test window, so the worker's own throttled wake-loop scan recovers it.
    await redis.zadd(wfActive, Date.now() - 60_000, id)
    await vi.waitFor(async () => {
      expect(await redis.hget(`${prefix}:${wfId}:j:${id}`, 'stalledCount')).toBe('1')
    })

    gate.open() // let the dead invocation "return" (its commit no-ops — the token was released)
    expect(await queue.wait(id)).toBe('ok:2') // requeued job re-runs and commits
    expect(calls).toBe(2)
  } finally {
    await ns.close()
  }
})

test('heartbeat renewal keeps a healthy long-running job out of the stalled window', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })
  const wfActive = `${prefix}:${wfId}:active`

  let calls = 0
  const started = makeGate()
  const gate = makeGate()
  queue.worker(
    async () => {
      calls++
      if (calls === 1) {
        started.open()
        await gate.wait()
      }
      return 'ok'
    },
    // Short lock ⇒ heartbeat every ~100ms and scans every 50ms. A broken heartbeat would let
    // the deadline fall into the past and the job would be recovered + re-run.
    { lockMs: 300, safetyTimeout: 0.1, stalledInterval: 50 },
  )

  try {
    const { id } = await queue.add('x')
    await started.wait()

    // Work well past lockMs while the handler is still busy; the heartbeat must keep the
    // deadline in the future so every throttled scan finds no candidate.
    await sleep(900)
    expect(Number(await redis.zscore(wfActive, id))).toBeGreaterThan(Date.now())
    expect(calls).toBe(1) // never falsely recovered / re-run
    expect(await redis.hget(`${prefix}:${wfId}:j:${id}`, 'stalledCount')).toBe('0')

    gate.open()
    expect(await queue.wait(id)).toBe('ok')
    expect(calls).toBe(1)
  } finally {
    await ns.close()
  }
})

test('recovery requeues at the original score (front of band) and unblocks the group', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId, groupConcurrency: 1 })
  const wfActive = `${prefix}:${wfId}:active`

  const order: string[] = []
  const startedA = makeGate()
  const gate = makeGate()
  let blocked = false
  queue.worker(
    async (job) => {
      order.push(job.data)
      if (job.data === 'A' && !blocked) {
        blocked = true
        startedA.open()
        await gate.wait() // A's first invocation "dies"
      }
      return 'ok'
    },
    { concurrency: 1, safetyTimeout: 0.2, stalledInterval: 100 },
  )

  try {
    const { id: aId } = await queue.add('A', { groupId: 'g' })
    await queue.add('B', { groupId: 'g' }) // stuck behind A (group cap 1)
    await startedA.wait() // A in flight; B cannot run — the group is wedged on the dead A

    await redis.zadd(wfActive, Date.now() - 60_000, aId) // A's deadline expires
    await vi.waitFor(async () => {
      expect(await redis.hget(`${prefix}:${wfId}:j:${aId}`, 'stalledCount')).toBe('1')
    })
    gate.open()

    // A requeued at its ORIGINAL score (front of its band) ⇒ it re-runs before B; the group
    // is unblocked so B runs too. A fresh counter would have ordered A behind B (['A','B','A']).
    await vi.waitFor(() => {
      expect(order).toEqual(['A', 'A', 'B'])
    })
  } finally {
    await ns.close()
  }
})

test('a job in state=completing is fenced from recovery', async () => {
  const prefix = randomUUID()
  const nsId = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
  const jobId = randomUUID()
  const jobKey = `${prefix}:${wfId}:j:${jobId}`

  try {
    // A job mid-complete: state=completing (the fence set before releaseActive) with an already
    // expired deadline. Recovery must skip it, closing the complete-vs-recover race.
    // prettier-ignore
    await redis.hset(jobKey, 'state', 'completing', 'groupId', 'g', 'nsId', nsId, 'stalledCount', '0', 'score', '123')
    await redis.zadd(`${prefix}:${wfId}:active`, Date.now() - 60_000, jobId)

    const recovered = await ns.redis.recoverStalled(prefix, wfId, 1, 1, 1000, 500, 300, 100)
    expect(recovered).toBe(0)
    expect(await redis.hget(jobKey, 'stalledCount')).toBe('0') // budget untouched
    expect(await redis.hget(jobKey, 'state')).toBe('completing') // state unchanged
    expect(await redis.zscore(`${prefix}:${wfId}:g:g:jobs`, jobId)).toBeNull() // not requeued
  } finally {
    await ns.close()
  }
})

test('a job past maxStalledCount is dead-lettered instead of requeued', async () => {
  const prefix = randomUUID()
  const nsId = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
  const jobId = randomUUID()
  const jobKey = `${prefix}:${wfId}:j:${jobId}`
  const wfActive = `${prefix}:${wfId}:active`

  try {
    // Already at the budget (maxStalledCount 1); the next stall (→ 2) exceeds it.
    // prettier-ignore
    await redis.hset(jobKey, 'state', 'active', 'groupId', 'g', 'nsId', nsId, 'stalledCount', '1', 'score', '123', 'maxAttempts', '1')
    await redis.zadd(wfActive, Date.now() - 60_000, jobId)

    const recovered = await ns.redis.recoverStalled(prefix, wfId, 1, 1, 1000, 500, 300, 100)
    expect(recovered).toBe(1)

    // Routed through the shared moveToFailed/finalizeFailed dead-letter path (bypasses retry).
    expect(await redis.zscore(`${prefix}:${wfId}:failed`, jobId)).not.toBeNull()
    expect(await redis.hget(jobKey, 'state')).toBe('failed')
    expect(await redis.hget(jobKey, 'failedReason')).toBe('stalled more than allowable limit')
    expect(await redis.hget(jobKey, 'stalledCount')).toBe('2')
    expect(await redis.zcard(wfActive)).toBe(0) // claim released
    expect(await redis.zscore(`${prefix}:${wfId}:g:g:jobs`, jobId)).toBeNull() // not requeued
  } finally {
    await ns.close()
  }
})

test('abort-on-lost-claim: a worker whose token is stolen aborts ctx.signal and does not commit', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })

  let aborted = false
  const started = makeGate()
  queue.worker(
    async (_job, ctx) => {
      started.open()
      await Promise.race([
        new Promise<void>((resolve) => {
          if (ctx.signal.aborted) resolve()
          else ctx.signal.addEventListener('abort', () => resolve())
        }),
        sleep(3000), // fail-safe so a broken abort surfaces as a failed assertion, not a hang
      ])
      aborted = ctx.signal.aborted
      return 'should-not-commit'
    },
    // Short lock ⇒ heartbeat ~100ms for quick lost-claim detection; long re-poll so no
    // background scan recovers the (post-theft) claim mid-test.
    { lockMs: 300, safetyTimeout: 30 },
  )

  try {
    const { id } = await queue.add('x')
    await started.wait()

    // Steal the claim: overwrite the lock with a foreign token, as recovery + re-reserve would.
    const lockKey = `${prefix}:${wfId}:j:${id}:lock`
    await redis.set(lockKey, 'foreign-token')

    await vi.waitFor(() => {
      expect(aborted).toBe(true) // heartbeat CAS returned 0 ⇒ worker aborted ctx.signal
    })
    // The old worker dropped the job without committing (no result written).
    expect(await redis.exists(`${prefix}:${wfId}:result:${id}`)).toBe(0)
    // The commit stays token-safe: the foreign holder can still finalize the claim.
    const record = JSON.stringify({ state: 'completed', value: 'by-recoverer' })
    expect(await ns.redis.complete(prefix, wfId, id, 'foreign-token', record, 300, 1)).toBe(1)
  } finally {
    await ns.close()
  }
})

// ---------------------------------------------------------------------------
// Cron scheduling (ticket 09)
// ---------------------------------------------------------------------------
// No fake clock: firing is driven deterministically by BACK-DATING the `schedules:due`
// score so a schedule is immediately due, never by waiting for a real cron tick.

test('upsertSchedule is idempotent: upserting the same id twice leaves one due entry + one hash', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })

  try {
    await queue.upsertSchedule('nightly', { pattern: '* * * * *', data: 'a', tz: 'UTC' })
    await queue.upsertSchedule('nightly', { pattern: '*/5 * * * *', data: 'b', tz: 'UTC' })

    // Keyed by (wfId, scheduleId): the second upsert replaces in place — no duplication.
    expect(await redis.zcard(`${prefix}:${wfId}:schedules:due`)).toBe(1)
    const scheduleKeys = await redis.keys(`${prefix}:${wfId}:schedule:*`)
    expect(scheduleKeys).toHaveLength(1)

    const schedules = await queue.getSchedules()
    expect(schedules).toHaveLength(1)
    expect(schedules[0]!.scheduleId).toBe('nightly')
    expect(schedules[0]!.pattern).toBe('*/5 * * * *') // replaced, not duplicated
    expect(schedules[0]!.tz).toBe('UTC')
    expect(schedules[0]!.nextRun).toBeGreaterThan(Date.now())
    expect(schedules[0]!.lastFireAt).toBeNull()
    expect(schedules[0]!.lastJobId).toBeNull()
  } finally {
    await ns.close()
  }
})

test('cron end-to-end: a worker tick fires a due schedule into a running occurrence', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })
  const dueKey = `${prefix}:${wfId}:schedules:due`

  const runs = vi.fn()
  const seenGroups = new Set<string>()

  try {
    await queue.upsertSchedule('sched', { pattern: '* * * * *', data: 'tick', tz: 'UTC' })
    // Back-date the due score so the schedule is immediately due on the worker's next tick.
    await redis.zadd(dueKey, Date.now() - 60_000, 'sched')

    queue.worker(
      (job) => {
        seenGroups.add(job.groupId)
        runs(job.data)
        return 'ok'
      },
      { safetyTimeout: 0.1 },
    )

    await vi.waitFor(() => {
      expect(runs).toHaveBeenCalledTimes(1)
    })
    await sleep(200)
    expect(runs).toHaveBeenCalledTimes(1) // the wake-loop tick fired the occurrence once
    expect(runs).toHaveBeenCalledWith('tick')
    expect(seenGroups).toEqual(new Set(['sched'])) // default groupId = scheduleId

    // Re-armed forward to the next occurrence — a single future due entry.
    expect(await redis.zcard(dueKey)).toBe(1)
    expect(Number(await redis.zscore(dueKey, 'sched'))).toBeGreaterThan(Date.now())
  } finally {
    await ns.close()
  }
})

test('cron fires exactly once under concurrent fire (CAS on score): one fired, one stale', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const nsId = randomUUID()
  const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })
  const dueKey = `${prefix}:${wfId}:schedules:due`
  const groupJobs = `${prefix}:${wfId}:g:sched:jobs`

  try {
    await queue.upsertSchedule('sched', { pattern: '* * * * *', data: 'tick', tz: 'UTC' })
    // Two workers that both read the SAME due entry compute the same `expectedScore` and fire
    // concurrently. Redis serializes the two atomic scripts: the first CAS wins and advances the
    // score, so the second finds `ZSCORE != expectedScore` and no-ops ⇒ exactly-once, no lock.
    const expected = Date.now() - 60_000
    await redis.zadd(dueKey, expected, 'sched')
    const next = Date.now() + 60_000

    const [a, b] = await Promise.all([
      ns.redis.fireSchedule(prefix, wfId, nsId, 'sched', expected, next, 'occ-a', 1, 1),
      ns.redis.fireSchedule(prefix, wfId, nsId, 'sched', expected, next, 'occ-b', 1, 1),
    ])

    // Exactly one enqueued the occurrence; the other saw the re-armed score and bailed stale.
    expect([a[0], b[0]].sort()).toEqual(['fired', 'stale'])
    expect(await redis.zcard(groupJobs)).toBe(1) // a single occurrence waiting, never two
    // Re-armed exactly once, forward to the next occurrence.
    expect(await redis.zcard(dueKey)).toBe(1)
    expect(Number(await redis.zscore(dueKey, 'sched'))).toBe(next)
  } finally {
    await ns.close()
  }
})

test('a long-overdue cron fires once then jumps forward (missed-run = skip, no stampede)', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })
  const dueKey = `${prefix}:${wfId}:schedules:due`

  const runs = vi.fn()
  try {
    await queue.upsertSchedule('overdue', { pattern: '* * * * *', data: 'x', tz: 'UTC' })
    // A whole hour of missed minute-occurrences — a backlog that must NOT stampede.
    await redis.zadd(dueKey, Date.now() - 3_600_000, 'overdue')

    queue.worker(
      () => {
        runs()
        return 'ok'
      },
      { safetyTimeout: 0.1 },
    )

    await vi.waitFor(() => {
      expect(runs).toHaveBeenCalledTimes(1)
    })
    await sleep(200)
    // `nextRun(now)` collapses the backlog: one fire, not ~60, and re-armed into the future.
    expect(runs).toHaveBeenCalledTimes(1)
    expect(Number(await redis.zscore(dueKey, 'overdue'))).toBeGreaterThan(Date.now())
  } finally {
    await ns.close()
  }
})

test('skip-if-running: an occurrence is not enqueued while lastJobId is non-terminal, but re-arms', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const nsId = randomUUID()
  const ns = new Namespace({ id: nsId, redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })
  const dueKey = `${prefix}:${wfId}:schedules:due`
  const scheduleKey = `${prefix}:${wfId}:schedule:sched`
  const groupJobs = `${prefix}:${wfId}:g:sched:jobs`

  try {
    await queue.upsertSchedule('sched', { pattern: '* * * * *', data: 'x', tz: 'UTC' })
    // Pretend the previous occurrence is still in flight (a non-terminal active job hash).
    await redis.hset(scheduleKey, 'lastJobId', 'prev-job')
    await redis.hset(`${prefix}:${wfId}:j:prev-job`, 'state', 'active')

    const past = Date.now() - 60_000
    await redis.zadd(dueKey, past, 'sched')
    const nextScore = Date.now() + 60_000

    // Direct CAS call (deterministic, no worker): the previous job is running ⇒ skip enqueue,
    // but the score still advances so the schedule stays armed.
    const skipped = await ns.redis.fireSchedule(
      prefix,
      wfId,
      nsId,
      'sched',
      past,
      nextScore,
      'occ-1',
      1,
      1,
    )
    expect(skipped[0]).toBe('skipped')
    expect(Number(await redis.zscore(dueKey, 'sched'))).toBe(nextScore) // re-armed anyway
    expect(await redis.hget(scheduleKey, 'lastJobId')).toBe('prev-job') // unchanged, nothing enqueued
    expect(await redis.zcard(groupJobs)).toBe(0) // no occurrence waiting

    // Once the previous job is terminal (completed ⇒ its hash is DEL'd), the next fire enqueues.
    await redis.del(`${prefix}:${wfId}:j:prev-job`)
    const nextScore2 = nextScore + 60_000
    const fired = await ns.redis.fireSchedule(
      prefix,
      wfId,
      nsId,
      'sched',
      nextScore,
      nextScore2,
      'occ-2',
      1,
      1,
    )
    expect(fired[0]).toBe('fired')
    expect(fired[1]).toBe('occ-2')
    expect(await redis.hget(scheduleKey, 'lastJobId')).toBe('occ-2')
    expect(await redis.zscore(groupJobs, 'occ-2')).not.toBeNull() // enqueued into its group
  } finally {
    await ns.close()
  }
})

// ---------------------------------------------------------------------------
// Events & metrics (ticket 10)
// ---------------------------------------------------------------------------

test('getMetrics reports active/waiting/delayed; a drained group leaves the groups set', async () => {
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix })
  const queue = ns.queue({ id: wfId })
  const started = makeGate()
  const gate = makeGate()

  // A single-slot worker holds exactly one job active (gated) and never picks up the rest.
  queue.worker(
    async () => {
      started.open()
      await gate.wait()
      return 'ok'
    },
    { concurrency: 1 },
  )

  try {
    // This job is reserved and held active: reserving pops the last job from its group's waiting
    // ZSET, so the shared maintenance must SREM it from the `groups` set (drained, no over-count).
    await queue.add('active', { groupId: 'active-grp' })
    await started.wait()

    // Waiting work across two groups (worker is full ⇒ stays waiting), plus two future delayed jobs.
    await queue.add('g1-a', { groupId: 'g1' })
    await queue.add('g1-b', { groupId: 'g1' })
    await queue.add('g2-a', { groupId: 'g2' })
    await queue.add('d1', { runAt: Date.now() + 3_600_000 })
    await queue.add('d2', { runAt: Date.now() + 3_600_000 })

    await vi.waitFor(async () => {
      expect(await queue.getMetrics()).toEqual({ active: 1, waiting: 3, delayed: 2 })
    })

    // The drained active group is gone from the metrics set; only the two waiting groups remain
    // (delayed jobs never enter it). A missing SREM would leave `active-grp` here.
    const groups = await redis.smembers(`${prefix}:${wfId}:groups`)
    expect(groups.sort()).toEqual(['g1', 'g2'])
  } finally {
    gate.open()
    await ns.close()
  }
})

test('onFailed is invoked with the job and error each time a handler throws', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() })
  const onFailed = vi.fn<(job: ReservedJob, error: unknown) => void>()

  queue.worker(
    () => {
      throw new Error('boom')
    },
    { onFailed },
  )

  try {
    const { id } = await queue.add('x')
    await expect(queue.wait(id)).rejects.toThrow('boom')

    await vi.waitFor(() => {
      expect(onFailed).toHaveBeenCalledTimes(1)
    })
    const [job, error] = onFailed.mock.calls[0]!
    expect(job.id).toBe(id)
    expect(error).toBeInstanceOf(Error)
    expect((error as Error).message).toBe('boom')
  } finally {
    await ns.close()
  }
})

test('onError is invoked when a worker-internal operation throws', async () => {
  const ns = new Namespace({ id: randomUUID(), redis: await connect(), prefix: randomUUID() })
  const queue = ns.queue({ id: randomUUID() })
  const onError = vi.fn<(error: unknown) => void>()

  // Force the wake-loop's throttled stalled-recovery scan to throw — a worker-internal error path.
  vi.spyOn(ns.redis, 'recoverStalled').mockRejectedValue(new Error('scan failed'))

  queue.worker(() => 'ok', { onError, safetyTimeout: 0.1 })

  try {
    await vi.waitFor(() => {
      expect(onError).toHaveBeenCalled()
    })
    const error = onError.mock.calls[0]![0]
    expect(error).toBeInstanceOf(Error)
    expect((error as Error).message).toBe('scan failed')
  } finally {
    vi.restoreAllMocks()
    await ns.close()
  }
})
