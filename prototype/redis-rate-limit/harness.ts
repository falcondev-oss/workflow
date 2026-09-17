/* eslint-disable no-console */
/**
 * PROTOTYPE, throwaway. Drives the rate limit gate in `reserve` and the `rateLimit` op against a
 * real Redis and prints the state after each step.
 *
 * Run: `pnpm tsx prototype/redis-rate-limit/harness.ts`
 */

import type { QueueRedis } from '../../src/queue/scripts'
import type { RateLimiterBudget } from '../../src/queue/types'
import { randomUUID } from 'node:crypto'
import { sleep } from '@antfu/utils'
import { RedisContainer } from '@testcontainers/redis'
import { createRedis } from '../../src'
import { Namespace, RateLimitError } from '../../src/queue'

const container = await new RedisContainer('redis:7-alpine').start()
const addr = { host: container.getHost(), port: container.getPort() }
const namespaces: Namespace[] = []

let failures = 0
function check(label: string, ok: boolean, detail = '') {
  if (!ok) failures++
  console.log(`  ${ok ? 'ok  ' : 'FAIL'} ${label}${detail ? `  (${detail})` : ''}`)
}

async function makeNs(
  nsId: string,
  prefix: string,
  rateLimiters: Record<string, RateLimiterBudget>,
) {
  const ns = new Namespace({ id: nsId, prefix, redis: await createRedis(addr), rateLimiters })
  namespaces.push(ns)
  return ns
}

async function rawReserve(
  ns: Namespace,
  wfId: string,
  limiterArgs: (string | number)[],
  want: number,
) {
  const res = await ns.redis.reserve(
    ns.prefix,
    wfId,
    ns.id,
    Number.MAX_SAFE_INTEGER,
    Number.MAX_SAFE_INTEGER,
    1,
    30_000,
    randomUUID(),
    500,
    want,
    ...limiterArgs,
  )
  const [jobs, msToNext] = res
  return {
    claimed: jobs.length,
    jobIds: jobs.map((j) => j[0]),
    msToNext,
    maxed: res[4],
    msToLimit: res[5],
  }
}

function logKey(ns: Namespace, name: string) {
  return `${ns.prefix}:ns:${ns.id}:rl:${name}`
}

async function logState(redis: QueueRedis, key: string) {
  const [time] = await redis.time()
  const nowMs = Number(time) * 1000
  const scores = await redis.zrange(key, 0, -1, 'WITHSCORES')
  const expiresIn = scores.filter((_, i) => i % 2 === 1).map((s) => Number(s) - nowMs)
  return {
    starts: expiresIn.length,
    expiresInMs: `${Math.min(...expiresIn)}..${Math.max(...expiresIn)}`,
    pttl: await redis.pttl(key),
  }
}

async function scenarioBatch() {
  console.log('\n[1] one reserve claims a batch and stops at the limit')
  const ns = await makeNs(randomUUID(), randomUUID(), {
    a: { limit: 5, window: 1000 },
    b: { limit: 3, window: 300 },
  })
  const q = ns.queue({ id: 'wf', rateLimiters: ['a'] })
  for (let i = 0; i < 12; i++) await q.add(`j${i}`)

  const first = await rawReserve(ns, 'wf', q.rateLimitArgs, 20)
  console.log('  reserve(want=20):', first)
  console.log('  log a:', await logState(ns.redis, logKey(ns, 'a')))
  check('claims exactly limit', first.claimed === 5)
  check(
    'maxed, with a timer near the window',
    first.maxed === 1 && first.msToLimit > 950 && first.msToLimit <= 1000,
    `msToLimit=${first.msToLimit}`,
  )

  const second = await rawReserve(ns, 'wf', q.rateLimitArgs, 20)
  console.log('  reserve again:', second)
  check(
    'full limiter claims nothing',
    second.claimed === 0 && second.maxed === 1 && second.msToLimit > 0,
  )

  const saturated = await rawReserve(ns, 'wf', q.rateLimitArgs, 0)
  check(
    'want=0 skips the gate',
    saturated.msToLimit === -1 && saturated.maxed === 0,
    JSON.stringify(saturated),
  )

  const q2 = ns.queue({ id: 'wf2', rateLimiters: ['a', 'b'] })
  const empty = await rawReserve(ns, 'wf2', q2.rateLimitArgs, 5)
  check(
    'empty queue never reads the gate',
    empty.msToLimit === -1 && empty.maxed === 0,
    JSON.stringify(empty),
  )

  await sleep(1000)
  const both = await rawReserve(ns, 'wf', ['2', 'a', 5, 1000, 'b', 3, 300], 20)
  console.log('  reserve with a (5/1000) and b (3/300):', both)
  check('tightest limiter caps the batch', both.claimed === 3)
  check(
    'timer is the tighter limiter',
    both.msToLimit > 250 && both.msToLimit <= 300,
    `msToLimit=${both.msToLimit}`,
  )
  console.log(
    '  log a:',
    await logState(ns.redis, logKey(ns, 'a')),
    'log b:',
    await logState(ns.redis, logKey(ns, 'b')),
  )

  await sleep(310)
  const afterB = await rawReserve(ns, 'wf', ['2', 'a', 5, 1000, 'b', 3, 300], 20)
  console.log('  after b frees, a still has 2 left:', afterB)
  check(
    'a caps the next batch at its remaining 2',
    afterB.claimed === 2 && afterB.msToLimit > 300,
    `msToLimit=${afterB.msToLimit}`,
  )
}

async function scenarioLoad() {
  console.log('\n[2] exactness and wake cost: 2 workflows x 3 worker processes share one limiter')
  const nsId = randomUUID()
  const prefix = randomUUID()
  const limit = 20
  const window = 500
  const perWf = 150
  const starts: number[] = []
  let reserves = 0
  let limitedReserves = 0
  let wastedWakes = 0

  const workers = []
  for (let p = 0; p < 3; p++) {
    const ns = await makeNs(nsId, prefix, { api: { limit, window } })
    const reserve = ns.redis.reserve.bind(ns.redis)
    ns.redis.reserve = async (...args) => {
      const res = await reserve(...args)
      reserves++
      if (res[5] > 0) limitedReserves++
      if (res[0].length === 0 && res[5] > 0) wastedWakes++
      return res
    }
    for (const wfId of ['wf-a', 'wf-b']) {
      const q = ns.queue({ id: wfId, rateLimiters: ['api'] })
      workers.push(
        q.worker(
          async () => {
            starts.push(performance.now())
            await sleep(5)
            return 'ok'
          },
          { concurrency: 10 },
        ),
      )
    }
  }

  const producer = namespaces.at(-1)!
  const t0 = performance.now()
  await Promise.all(
    ['wf-a', 'wf-b'].flatMap((wfId) => {
      const q = producer.queue({ id: wfId, rateLimiters: ['api'] })
      return Array.from({ length: perWf }, async (_, i) => q.add(String(i)))
    }),
  )
  while (starts.length < perWf * 2) await sleep(20)
  const elapsed = performance.now() - t0
  await Promise.all(workers.map(async (w) => w.close()))

  starts.sort((a, b) => a - b)
  const maxIn = (w: number) => {
    let best = 0
    for (let i = 0, j = 0; i < starts.length; i++) {
      while (starts[i]! - starts[j]! >= w) j++
      best = Math.max(best, i - j + 1)
    }
    return best
  }
  const ideal = ((perWf * 2) / limit - 1) * window
  console.log(`  ${starts.length} starts in ${elapsed.toFixed(0)} ms (ideal ~${ideal} ms)`)
  console.log(
    `  reserve calls: ${reserves}, limited: ${limitedReserves}, limited with 0 claims: ${wastedWakes}`,
  )
  // Handler start times are local and jittered by a few ms against the Redis clock that decides.
  console.log(
    `  max starts in any ${window} ms: ${maxIn(window)}, in any ${window - 10} ms: ${maxIn(window - 10)}`,
  )
  check('never more than limit in a window (10 ms jitter allowance)', maxIn(window - 10) <= limit)
  check(
    'drains close to the ideal rate',
    elapsed < ideal + window * 1.5,
    `${elapsed.toFixed(0)} ms`,
  )
}

async function scenarioSpacing() {
  console.log('\n[2b] even spacing: limit 1, window 100')
  const ns = await makeNs(randomUUID(), randomUUID(), { api: { limit: 1, window: 100 } })
  const q = ns.queue({ id: 'wf', rateLimiters: ['api'] })
  const starts: number[] = []
  for (let i = 0; i < 30; i++) await q.add(String(i))
  const t0 = performance.now()
  const worker = q.worker(
    () => {
      starts.push(performance.now() - t0)
      return 'ok'
    },
    { concurrency: 5 },
  )
  while (starts.length < 30) await sleep(10)
  await worker.close()
  const gaps = starts.slice(1).map((s, i) => s - starts[i]!)
  console.log(
    `  30 starts in ${starts.at(-1)!.toFixed(0)} ms (ideal 2900), gap min=${Math.min(...gaps).toFixed(0)} max=${Math.max(...gaps).toFixed(0)}`,
  )
  check('gaps stay near 100 ms', Math.min(...gaps) >= 95 && Math.max(...gaps) < 130)
}

async function scenarioPause() {
  console.log('\n[3] rateLimit(ms, name) pauses every workflow on the limiter and requeues the job')
  const ns = await makeNs(randomUUID(), randomUUID(), { api: { limit: 100, window: 1000 } })
  const qa = ns.queue({ id: 'wf-a', rateLimiters: ['api'] })
  const qb = ns.queue({ id: 'wf-b', rateLimiters: ['api'] })
  const log: string[] = []
  const t0 = performance.now()
  const at = () => `${(performance.now() - t0).toFixed(0)}ms`

  const a1 = await qa.add('a1', { groupId: 'g' })
  await qa.add('a2', { groupId: 'g' })
  await ns.redis.hset(`${ns.prefix}:wf-a:j:${a1.id}:steps`, 'fetch', '"cached"')

  let thrown = false
  qa.worker(async (job) => {
    log.push(
      `${at()} wf-a ${job.data} attempt=${job.attemptsMade} steps=${[...job.steps.keys()].join(',')}`,
    )
    if (job.data === 'a1' && !thrown) {
      thrown = true
      throw new RateLimitError(400, 'api')
    }
    return 'ok'
  })
  qb.worker((job) => {
    log.push(`${at()} wf-b ${job.data}`)
    return 'ok'
  })

  const pausedKey = `${logKey(ns, 'api')}:paused`
  // A pause only closes the gate for starts after it, so enqueue wf-b's job once it is written.
  while ((await ns.redis.exists(pausedKey)) === 0) await sleep(1)
  log.push(`${at()} pause written, enqueue b1`)
  await qb.add('b1')
  await qa.wait(a1.id)
  await sleep(100)
  for (const line of log) console.log(`  ${line}`)
  console.log('  pause key after it ended:', await ns.redis.get(pausedKey))
  const bStart = Number(log.find((l) => l.includes('wf-b'))?.split('ms')[0])
  check('wf-b does not start before the pause ends', bStart >= 390, `${bStart}ms`)
  const rerun = log.filter((l) => l.includes('wf-a'))[1]
  check(
    'a1 reruns first in its group, attempt 0, steps kept',
    rerun?.includes('wf-a a1 attempt=0 steps=fetch') ?? false,
    rerun,
  )
  check('rerun waits for the pause too', Number(rerun?.split('ms')[0]) >= 390, rerun)
  check('pause key expired on its own', (await ns.redis.exists(pausedKey)) === 0)
}

async function scenarioPauseOps() {
  console.log('\n[4] pause op edge cases')
  const ns = await makeNs(randomUUID(), randomUUID(), { api: { limit: 100, window: 1000 } })
  const q = ns.queue({ id: 'wf', rateLimiters: ['api'] })
  const self = ns.queue({ id: 'self' })
  const other = ns.queue({ id: 'other' })
  const pausedKey = `${logKey(ns, 'api')}:paused`

  const job = await q.add('x')
  const [claim] = (await rawReserve(ns, 'wf', q.rateLimitArgs, 1)).jobIds
  const stale = await ns.redis.rateLimit(
    ns.prefix,
    'wf',
    ns.id,
    claim!,
    'wrong-token',
    500,
    'api',
    1,
  )
  const state = await ns.redis.hmget(`${ns.prefix}:wf:j:${job.id}`, 'state', 'attempts')
  console.log('  stale token:', {
    returned: stale,
    pausedUntilIn: Number(await ns.redis.get(pausedKey)) - Date.now(),
    job: state,
  })
  check(
    'stale token writes the pause but leaves the job active',
    stale === 0 && state[0] === 'active' && (await ns.redis.exists(pausedKey)) === 1,
  )

  const before = await ns.redis.get(pausedKey)
  await ns.redis.rateLimit(ns.prefix, 'wf', ns.id, claim!, 'wrong-token', 100, 'api', 1)
  check(
    'a shorter pause never shortens the existing one',
    (await ns.redis.get(pausedKey)) === before,
  )
  await ns.redis.rateLimit(ns.prefix, 'wf', ns.id, claim!, 'wrong-token', 2000, 'api', 1)
  check('a longer pause extends it', Number(await ns.redis.get(pausedKey)) > Number(before))

  console.log('  workflow with no limiters pauses only itself')
  await self.add('s1')
  await other.add('o1')
  const selfClaim = (await rawReserve(ns, 'self', self.rateLimitArgs, 1)).jobIds[0]!
  const token = await ns.redis.get(`${ns.prefix}:self:j:${selfClaim}:lock`)
  const requeued = await ns.redis.rateLimit(ns.prefix, 'self', ns.id, selfClaim, token!, 300, '', 1)
  const selfAgain = await rawReserve(ns, 'self', self.rateLimitArgs, 1)
  const otherRes = await rawReserve(ns, 'other', other.rateLimitArgs, 1)
  console.log('  ', { requeued, selfAgain, otherRes })
  check(
    'self is paused with a timer',
    requeued === 1 && selfAgain.claimed === 0 && selfAgain.msToLimit > 250,
  )
  check('other workflow in the namespace still claims', otherRes.claimed === 1)
}

async function scenarioMixedWindows() {
  console.log('\n[5] rolling deploy: two pods disagree on the window')
  const nsId = randomUUID()
  const prefix = randomUUID()
  const long = await makeNs(nsId, prefix, { api: { limit: 3, window: 1000 } })
  const short = await makeNs(nsId, prefix, { api: { limit: 3, window: 200 } })
  const ql = long.queue({ id: 'wf', rateLimiters: ['api'] })
  const qs = short.queue({ id: 'wf', rateLimiters: ['api'] })
  for (let i = 0; i < 20; i++) await ql.add(String(i))

  const l1 = await rawReserve(long, 'wf', ql.rateLimitArgs, 10)
  await sleep(250)
  const s1 = await rawReserve(short, 'wf', qs.rateLimitArgs, 10)
  console.log('  long pod first, short pod 250 ms later:', { long: l1.claimed, short: s1 })
  check("short pod does not prune the long pod's starts", s1.claimed === 0 && s1.msToLimit > 650)

  await sleep(1000)
  const s2 = await rawReserve(short, 'wf', qs.rateLimitArgs, 10)
  await sleep(250)
  const l2 = await rawReserve(long, 'wf', ql.rateLimitArgs, 10)
  console.log('  short pod first, long pod 250 ms later:', { short: s2.claimed, long: l2.claimed })
  console.log(
    '  => 6 starts within 250 ms: the looser budget wins until the deploy finishes (as specified)',
  )
  const ttl = await long.redis.pttl(logKey(long, 'api'))
  check("log TTL follows the latest expiry, not the writer's window", ttl > 900, `pttl=${ttl}`)
}

async function scenarioGroupHead() {
  console.log('\n[6] a limited job keeps its group position')
  const ns = await makeNs(randomUUID(), randomUUID(), { api: { limit: 1, window: 300 } })
  const q = ns.queue({ id: 'wf', rateLimiters: ['api'], groupConcurrency: 2 })
  await q.add('j1', { groupId: 'g' })
  const j2 = await q.add('j2', { groupId: 'g' })
  const first = await rawReserve(ns, 'wf', q.rateLimitArgs, 5)
  const groupJobs = await ns.redis.zrange(`${ns.prefix}:wf:g:g:jobs`, 0, -1)
  const state = await ns.redis.hget(`${ns.prefix}:wf:j:${j2.id}`, 'state')
  console.log('  ', { first, groupJobs, j2State: state })
  check(
    'only one start, j2 still waiting at its group head',
    first.claimed === 1 && groupJobs[0] === j2.id && state === 'waiting',
  )
}

try {
  await scenarioBatch()
  await scenarioLoad()
  await scenarioSpacing()
  await scenarioPause()
  await scenarioPauseOps()
  await scenarioMixedWindows()
  await scenarioGroupHead()
} finally {
  await Promise.all(namespaces.map(async (ns) => ns.close()))
  await container.stop({ remove: true, removeVolumes: true })
}
console.log(failures === 0 ? '\nall checks passed' : `\n${failures} check(s) failed`)
process.exit(failures === 0 ? 0 : 1)
