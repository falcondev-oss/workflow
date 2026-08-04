/**
 * End-to-end benchmark. Spins a real Redis (testcontainers, same image as the tests) and drives
 * the public API — `run()` / `work()` / `job.wait()` — so every number includes serialization,
 * the Lua round-trips, the worker wake loop and pub/sub delivery.
 *
 * Run: `pnpm bench`.
 */

import { randomUUID } from 'node:crypto'
import { RedisContainer } from '@testcontainers/redis'
import { createRedis, WorkflowNamespace } from '../src'

/** Runs per scenario. Throughput on a shared box is noisy; the best run is the least polluted. */
const RUNS = 3

function report(name: string, value: string, note: string) {
  console.log(`  ${name.padEnd(34)} ${value.padStart(10)} ${note}`)
}

/** Best of `RUNS` — the run least disturbed by the rest of the machine. */
async function best(name: string, run: () => Promise<number>) {
  const samples: number[] = []
  for (let i = 0; i < RUNS; i++) samples.push(await run())
  report(name, Math.max(...samples).toFixed(0), `ops/s (worst=${Math.min(...samples).toFixed(0)})`)
}

function quantile(sorted: number[], q: number) {
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * q))]!
}

interface RedisAddr {
  host: string
  port: number
}

/**
 * A namespace with a fresh id (key isolation) on its own connection — `namespace.close()`
 * disconnects the connection it was handed, so scenarios cannot share one.
 */
async function makeNamespace(addr: RedisAddr, opts?: { concurrency?: number }) {
  return new WorkflowNamespace({
    id: randomUUID(),
    redis: await createRedis(addr),
    autoClose: false,
    concurrency: opts?.concurrency,
    // A silently failing job would just stall the scenario forever — make it loud.
    logger: { error: console.error, warn: console.warn },
  })
}

/** Enqueue `count` jobs through `run()`, `parallel` at a time. */
async function enqueueAll(run: (i: number) => Promise<unknown>, count: number, parallel: number) {
  for (let i = 0; i < count; i += parallel) {
    await Promise.all(
      Array.from({ length: Math.min(parallel, count - i) }, async (_, k) => run(i + k)),
    )
  }
}

/** Resolves once `count` jobs have been observed by the worker handler. */
function makeCounter(count: number) {
  let seen = 0
  const { promise, resolve: done, reject: fail } = Promise.withResolvers<void>()
  // A job that dies (or never gets reserved) would otherwise hang the run forever.
  const timer = setTimeout(() => fail(new Error(`only ${seen}/${count} jobs ran`)), 120_000)
  return {
    hit: () => {
      if (++seen !== count) return
      clearTimeout(timer)
      done()
    },
    promise,
  }
}

/** Producer-side cost of `run()`: superjson + the `enqueue` script, no worker running. */
async function benchEnqueue(addr: RedisAddr) {
  const count = 5000
  const ns = await makeNamespace(addr)
  const wf = ns.createWorkflow({
    id: 'enqueue',
    schema: undefined,
    run: async () => 1,
  })

  await wf.run(undefined) // warm the lazy queue + script load

  for (const parallel of [1, 50]) {
    await best(`enqueue (${parallel === 1 ? 'serial' : `${parallel} inflight`})`, async () => {
      const start = performance.now()
      await enqueueAll(async () => wf.run(undefined), count, parallel)
      return (count / (performance.now() - start)) * 1000
    })
  }

  await ns.close()
}

interface DrainOptions {
  name: string
  count: number
  concurrency: number
  groups: number
  steps: number
}

/** Steady-state drain: jobs are all enqueued up front, then a worker chews through them. */
async function benchThroughput(addr: RedisAddr, opts: DrainOptions) {
  await best(opts.name, async () => runDrain(addr, opts))
}

async function runDrain(addr: RedisAddr, { count, concurrency, groups, steps }: DrainOptions) {
  const ns = await makeNamespace(addr)
  const counter = makeCounter(count)
  const wf = ns.createWorkflow({
    id: 'throughput',
    schema: undefined as never,
    async run({ step }) {
      for (let s = 0; s < steps; s++) await step.do(`s${s}`, () => s)
      counter.hit()
      return 1
    },
  })

  await enqueueAll(
    async (i) => wf.run({ i } as never, { groupId: groups === 0 ? undefined : `g${i % groups}` }),
    count,
    100,
  )

  const start = performance.now()
  const worker = await wf.work({ concurrency })
  await counter.promise
  const elapsed = performance.now() - start

  await worker.close()
  await ns.close()
  return (count / elapsed) * 1000
}

/** Full round trip a client actually feels: enqueue → execute → pub/sub notify → result read. */
async function benchLatency(addr: RedisAddr) {
  const count = 300
  const ns = await makeNamespace(addr)
  const wf = ns.createWorkflow({
    id: 'latency',
    schema: undefined as never,
    run: async () => 1,
  })
  const worker = await wf.work({ concurrency: 1 })

  const warmup = await wf.run(undefined as never)
  await warmup.wait()

  const samples: number[] = []
  for (let i = 0; i < count * RUNS; i++) {
    const start = performance.now()
    const job = await wf.run(undefined as never)
    await job.wait()
    samples.push(performance.now() - start)
  }
  samples.sort((a, b) => a - b)

  const mean = samples.reduce((a, b) => a + b, 0) / samples.length
  report(
    'run → wait round trip',
    mean.toFixed(2),
    `ms (p50=${quantile(samples, 0.5).toFixed(2)}, p99=${quantile(samples, 0.99).toFixed(2)})`,
  )

  await worker.close()
  await ns.close()
}

async function main() {
  const container = await new RedisContainer('redis:7-alpine').start()
  const addr = { host: container.getHost(), port: container.getPort() }

  try {
    console.log('\nbench: @falcondev-oss/workflow (e2e)\n')

    await benchEnqueue(addr)
    await benchThroughput(addr, {
      name: 'drain 1 worker c=1',
      count: 500,
      concurrency: 1,
      groups: 0,
      steps: 0,
    })
    await benchThroughput(addr, {
      name: 'drain 1 worker c=50',
      count: 3000,
      concurrency: 50,
      groups: 0,
      steps: 0,
    })
    await benchThroughput(addr, {
      name: 'drain c=50, 3 steps',
      count: 1000,
      concurrency: 50,
      groups: 0,
      steps: 3,
    })
    await benchThroughput(addr, {
      name: 'drain c=50, 10 groups',
      count: 1000,
      concurrency: 50,
      groups: 10,
      steps: 0,
    })
    await benchLatency(addr)
    console.log()
  } finally {
    await container.stop({ remove: true, removeVolumes: true })
  }
}

await main()
