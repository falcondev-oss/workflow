import type { StandardSchemaV1 } from '@standard-schema/spec'
import { randomUUID } from 'node:crypto'
import { sleep } from '@antfu/utils'
import { SpanStatusCode, trace } from '@opentelemetry/api'
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics'
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base'
import { type } from 'arktype'
import { stringify } from 'superjson'
import { beforeAll, describe, expect, onTestFinished, test, vi } from 'vitest'
import { z } from 'zod'
import { createRedis, ResultExpiredError, TimeoutError, WorkflowNamespace } from '../src'

let sharedRedis: Awaited<ReturnType<typeof createRedis>>

beforeAll(async () => {
  sharedRedis = await connect()
})

/** Mint a fresh namespace so each test is key-isolated by its random workflow/namespace ids. */
function namespace() {
  return new WorkflowNamespace({
    id: randomUUID(),
    redis: sharedRedis,
    logger: console,
    autoClose: false,
  })
}

async function connect() {
  return createRedis({ host: 'localhost', port: Number(process.env.REDIS_PORT) })
}

/** A one-shot latch: `open()` resolves everyone awaiting `wait()`. */
function makeGate() {
  let open!: () => void
  const promise = new Promise<void>((resolve) => {
    open = resolve
  })
  return { wait: async () => promise, open }
}

/** An identity Standard Schema — passes any value straight through (superjson-decoded input). */
function passthrough<T>(): StandardSchemaV1<T, T> {
  return {
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: (value) => ({ value: value as T }),
    },
  }
}

async function collect<T>(events: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = []
  for await (const event of events) result.push(event)
  return result
}

describe('input', () => {
  test('primitive', async () => {
    const handler = vi.fn()
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      schema: type({ name: 'string' }),
      run: handler,
    })
    await workflow.work()
    await workflow.run({ name: 'A' })

    await vi.waitFor(() =>
      expect(handler).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({ input: { name: 'A' } }),
      ),
    )
  })
  test('non-pojos', async () => {
    const handler = vi.fn()
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      schema: type({ date: 'Date' }),
      run: handler,
    })
    await workflow.work()
    await workflow.run({ date: new Date('2024-01-01T00:00:00.000Z') })

    await vi.waitFor(() =>
      expect(handler).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          // eslint-disable-next-line ts/no-unsafe-assignment
          input: { date: expect.any(Date) },
        }),
      ),
    )
  })
})

describe('wait', () => {
  test('returns the workflow output', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      schema: type({ n: 'number' }),
      run: async ({ input }) => ({ doubled: input.n * 2, at: new Date('2024-01-01') }),
    })
    await workflow.work()
    const job = await workflow.run({ n: 21 })

    await expect(job.wait()).resolves.toEqual({ doubled: 42, at: new Date('2024-01-01') })
  })

  test('throws the workflow failure, carrying its reason', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      run: async () => {
        throw new Error('boom')
      },
    })
    await workflow.work({ backoff: () => 0 })
    const job = await workflow.run(undefined)

    // The rejection is a real Error carrying the workflow's own failure reason as its message.
    const err = await job.wait().catch((err_: unknown) => err_)
    expect(err).toBeInstanceOf(Error)
    expect((err as Error).message).toBe('boom')
  })

  test('throws TimeoutError when the deadline elapses', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      run: async () => 'never', // no worker started ⇒ the job never finishes
    })
    const job = await workflow.run(undefined)

    await expect(job.wait(100)).rejects.toBeInstanceOf(TimeoutError)
  })

  test('throws ResultExpiredError when notified but the result is gone', async () => {
    // Drive under the seam exactly as the module test does: no worker ever writes a result, so a
    // `done` publish with no result key must surface as ResultExpiredError through WorkflowJob.wait.
    const redis = await connect()
    const prefix = randomUUID()
    const wfId = randomUUID()
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      redis,
      prefix,
      logger: console,
      autoClose: false,
    })
    const workflow = ns.createWorkflow({ id: wfId, run: async () => 'x' })

    try {
      const job = await workflow.run(undefined)
      const waiting = job.wait()
      // Race the subscribe registration: keep publishing `done` (no result key) until observed.
      const channel = `${prefix}:${wfId}:done:${job.id}`
      const pump = setInterval(() => void redis.publish(channel, '1'), 20)
      await expect(waiting).rejects.toBeInstanceOf(ResultExpiredError)
      clearInterval(pump)
    } finally {
      await ns.close()
    }
  })
})

describe('watch', () => {
  test('yields the attempt and output in emission order, then ends', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      run: async () => 'done',
    })
    const job = await workflow.run(undefined)
    const events = await job.watch()
    await workflow.work()

    const received = await collect(events)

    expect(received).toEqual([
      { type: 'started', attempt: 1 },
      { type: 'completed', output: 'done' },
    ])
  })

  test('yields a terminal failure instead of throwing from the stream', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      run: async () => {
        throw new Error('boom')
      },
    })
    const job = await workflow.run(undefined)
    const events = await job.watch()
    await workflow.work()

    const received = await collect(events)

    expect(received[0]).toEqual({ type: 'started', attempt: 1 })
    expect(received[1]).toMatchObject({ type: 'failed', error: { message: 'boom' } })
  })

  test('emits another start after a retry and no intermediate failure', async () => {
    let attempts = 0
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      workerOptions: { maxAttempts: 2 },
      run: async () => {
        if (attempts++ === 0) throw new Error('retry me')
        return 'done'
      },
    })
    const job = await workflow.run(undefined)
    const events = await job.watch()
    await workflow.work({ backoff: () => 0 })

    const received = await collect(events)

    expect(received).toEqual([
      { type: 'started', attempt: 1 },
      { type: 'started', attempt: 2 },
      { type: 'completed', output: 'done' },
    ])
  })

  test('gives two watchers the complete stream independently', async () => {
    const workflow = namespace().createWorkflow({ id: randomUUID(), run: async () => 'done' })
    const job = await workflow.run(undefined)
    const watchers = await Promise.all([job.watch(), job.watch()])
    await workflow.work()

    const received = await Promise.all(watchers.map(collect))

    expect(received[0]).toEqual(received[1])
    expect(received[0]).toEqual([
      { type: 'started', attempt: 1 },
      { type: 'completed', output: 'done' },
    ])
  })

  test('unsubscribes when iteration stops early', async () => {
    const gate = makeGate()
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      run: async () => {
        await gate.wait()
        return 'done'
      },
    })
    const job = await workflow.run(undefined)
    const [earlyEvents, completeEvents] = await Promise.all([job.watch(), job.watch()])
    await workflow.work()
    const completeReceived = collect(completeEvents)
    const earlyReceived = []

    try {
      for await (const event of earlyEvents) {
        earlyReceived.push(event)
        if (event.type === 'started') break
      }
    } finally {
      gate.open()
    }

    expect(earlyReceived).toEqual([{ type: 'started', attempt: 1 }])
    await expect(completeReceived).resolves.toEqual([
      { type: 'started', attempt: 1 },
      { type: 'completed', output: 'done' },
    ])
  })

  test('returns the terminal event when the job already finished', async () => {
    const workflow = namespace().createWorkflow({ id: randomUUID(), run: async () => 'done' })
    await workflow.work()
    const job = await workflow.run(undefined)
    await job.wait()

    const received = await collect(await job.watch())

    expect(received).toEqual([{ type: 'completed', output: 'done' }])
  })

  test('throws ResultExpiredError after the terminal result window', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      queueOptions: { resultTtl: 1 },
      run: async () => 'done',
    })
    const { job, events: initialEvents } = await workflow.runAndWatch(undefined)
    await workflow.work()
    await collect(initialEvents)
    await sleep(1100)

    const events = await job.watch()
    const next = events[Symbol.asyncIterator]().next()

    await expect(
      Promise.race([
        next,
        sleep(100).then(() => {
          throw new Error('watch hung')
        }),
      ]),
    ).rejects.toBeInstanceOf(ResultExpiredError)
  })

  test('accepts an AbortSignal', async () => {
    const workflow = namespace().createWorkflow({ id: randomUUID(), run: async () => 'never' })
    const job = await workflow.run(undefined)
    const controller = new AbortController()
    const events = await job.watch({ signal: controller.signal })
    const reason = new Error('stop watching')

    controller.abort(reason)

    await expect(events[Symbol.asyncIterator]().next()).rejects.toBe(reason)
  })

  test('validates and yields progress emitted from a nested step', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      schema: type({ name: 'string' }),
      progressSchema: type({ label: 'string', done: 'number' }),
      run: async ({ input, step }) => {
        input.name satisfies string
        await step.do('outer', async ({ step: nestedStep }) => {
          await nestedStep.progress({ label: 'Rendering', done: 1 })
          await nestedStep.progress({ label: 'Uploading', done: 2 })
        })
        return 'done'
      },
    })
    const job = await workflow.run({ name: 'report' })
    const events = await job.watch()
    await workflow.work()

    const received = await collect(events)

    expect(received).toEqual([
      { type: 'started', attempt: 1 },
      { type: 'progress', data: { label: 'Rendering', done: 1 } },
      { type: 'progress', data: { label: 'Uploading', done: 2 } },
      { type: 'completed', output: 'done' },
    ])
  })

  test('removes progress from workflows without a schema', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      run: async ({ step }) => {
        if (false) {
          // @ts-expect-error progress is unavailable without a progress schema
          await step.progress('undeclared')
        }
        return 'done'
      },
    })
    const job = await workflow.run(undefined)
    const events = await job.watch()
    await workflow.work()

    for await (const event of events) {
      // @ts-expect-error the progress arm vanishes when Progress is never
      if (event.type === 'progress') throw new Error('unreachable')
    }
  })

  test('splits schema input from validated output', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      schema: z.object({ id: z.string() }),
      progressSchema: z.object({
        value: z.string().transform(Number),
        label: z.string().default('Working'),
      }),
      run: async ({ input, step }) => {
        input.id satisfies string
        await step.progress({ value: '42' })
        return input.id
      },
    })
    const job = await workflow.run({ id: 'report' })
    const events = await job.watch()
    await workflow.work()

    for await (const event of events) {
      if (event.type === 'progress') {
        event.data.value satisfies number
        event.data.label satisfies string
        expect(event.data).toEqual({ value: 42, label: 'Working' })
      }
    }
  })

  test('keeps a union progress payload in the event type', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      progressSchema: type('string | number'),
      run: async ({ step }) => {
        await step.progress(1)
        return 'done'
      },
    })
    const job = await workflow.run(undefined)
    const events = await job.watch()
    await workflow.work()

    for await (const event of events) {
      if (event.type === 'progress') expect(event.data satisfies string | number).toBe(1)
    }
  })

  test('rejects progress that fails receive-side validation', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      progressSchema: type({ done: 'number' }),
      run: async ({ step }) => {
        await step.progress({ done: 'wrong' } as unknown as { done: number })
        return 'done'
      },
    })
    const job = await workflow.run(undefined)
    const events = await job.watch()
    const iterator = events[Symbol.asyncIterator]()
    await workflow.work()

    await expect(iterator.next()).resolves.toMatchObject({
      value: { type: 'started', attempt: 1 },
    })
    await expect(iterator.next()).rejects.toThrow(`Invalid workflow progress for job ${job.id}`)
  })

  test('does not re-emit progress from a cached step on retry', async () => {
    let attempts = 0
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      progressSchema: type({ label: 'string' }),
      workerOptions: { maxAttempts: 2 },
      run: async ({ step }) => {
        await step.do('cached', async ({ step: nestedStep }) => {
          await nestedStep.progress({ label: 'Running once' })
          return 'cached'
        })
        if (attempts++ === 0) throw new Error('retry')
        return 'done'
      },
    })
    const job = await workflow.run(undefined)
    const events = await job.watch()
    await workflow.work({ backoff: () => 0 })

    const received = await collect(events)

    expect(received).toEqual([
      { type: 'started', attempt: 1 },
      { type: 'progress', data: { label: 'Running once' } },
      { type: 'started', attempt: 2 },
      { type: 'completed', output: 'done' },
    ])
  })

  test('runAndWatch attaches before enqueueing', async () => {
    const workflow = namespace().createWorkflow({ id: randomUUID(), run: async () => 'done' })
    const { job, events } = await workflow.runAndWatch(undefined)
    await workflow.work()

    const received = await collect(events)

    expect(job.id).toMatch(/^[0-9a-f-]{36}$/i)
    expect(received).toEqual([
      { type: 'started', attempt: 1 },
      { type: 'completed', output: 'done' },
    ])
  })

  test('rehydrates a typed job handle from its id on a producer-only namespace', async () => {
    const workflowId = randomUUID()
    const producer = namespace().createWorkflow({ id: workflowId, run: async () => 'done' })
    const observer = namespace().createWorkflow({ id: workflowId, run: async () => 'unused' })
    const job = await producer.run(undefined)
    const attached = await observer.getJob(job.id)
    const watchers = await Promise.all([job.watch(), attached.watch()])
    await producer.work()

    const received = await Promise.all(watchers.map(collect))

    expect(attached.id).toBe(job.id)
    expect(received[0]).toEqual(received[1])
  })
})

describe('step', () => {
  test('only runs once across retries', async () => {
    const stepHandler1 = vi.fn()
    const stepHandler2 = vi.fn()
    const stepHandler3 = vi.fn()
    const handler = vi.fn()
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      workerOptions: { maxAttempts: 10 },
      run: async ({ step }) => {
        await Promise.all([
          step.do('test-step1', stepHandler1),
          step.do('test-step2', stepHandler2),
          step.do('test-step3', stepHandler3),
        ])
        await handler()
        throw new Error('error')
      },
    })
    await workflow.work({ backoff: () => 0 })

    await workflow.run(undefined)

    await vi.waitFor(() => {
      expect(stepHandler1).toHaveBeenCalledOnce()
      expect(stepHandler2).toHaveBeenCalledOnce()
      expect(stepHandler3).toHaveBeenCalledOnce()
      expect(handler).toHaveBeenCalledTimes(10)
    })
  })
  test('wait for running steps on job failure', async () => {
    const stepHandler1 = vi.fn(async () => sleep(1000))
    const stepHandler3 = vi.fn(async () => sleep(1000))
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      run: async ({ step }) => {
        await Promise.all([
          step.do('test-step1', stepHandler1),
          step.do('test-step2', () => {
            throw new Error('step error')
          }),
          step.do('test-step3', stepHandler3),
        ])
      },
    })

    const failedHandler = vi.fn()
    await workflow.work({ backoff: () => 0, onFailed: failedHandler })

    await workflow.run(undefined)

    await vi.waitFor(
      () => {
        expect(failedHandler).toHaveBeenCalled()
      },
      { timeout: 5000 },
    )
    expect(stepHandler1).toHaveResolved()
    expect(stepHandler3).toHaveResolved()
  })
  test('job retry replays completed steps and re-runs only the failed step', async () => {
    const step1 = vi.fn(() => 'one')
    let step2Attempt = 0
    const step2 = vi.fn(() => {
      // Fails on the first attempt only; the job-level retry must re-run just this step.
      if (step2Attempt++ === 0) throw new Error('step2 boom')
      return 'two'
    })
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      workerOptions: { maxAttempts: 3 },
      run: async ({ step }) => {
        const a = await step.do('s1', step1)
        const b = await step.do('s2', step2)
        return { a, b }
      },
    })
    await workflow.work({ backoff: () => 0 })
    const job = await workflow.run(undefined)

    await expect(job.wait()).resolves.toEqual({ a: 'one', b: 'two' })
    expect(step1).toHaveBeenCalledOnce() // completed step is NOT re-executed on replay
    expect(step2).toHaveBeenCalledTimes(2) // only the failed step re-ran
  })

  test('step.do has no step-level retry — a throw fails the whole job (default maxAttempts 1)', async () => {
    const stepHandler = vi.fn(() => {
      throw new Error('flaky')
    })
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      run: async ({ step }) => step.do('s', stepHandler),
    })
    await workflow.work() // maxAttempts default 1 ⇒ no retry at all

    const job = await workflow.run(undefined)
    await expect(job.wait()).rejects.toThrow('flaky')
    // A retired step-level retry would have re-invoked the handler; it runs exactly once.
    expect(stepHandler).toHaveBeenCalledOnce()
  })

  test('caches output', async () => {
    const stepHandler = vi.fn(() => ({
      primitive: 42,
      date: new Date(),
    }))
    const handler = vi.fn()
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      workerOptions: { maxAttempts: 3 },
      run: async ({ step }) => {
        const result = await step.do('test-step', stepHandler)
        await handler(result)
        throw new Error('error')
      },
    })
    await workflow.work({ backoff: () => 0 })

    await workflow.run(undefined)

    await vi.waitFor(() => {
      expect(stepHandler).toHaveBeenCalledOnce()
      expect(handler).toHaveBeenCalledTimes(3)
      expect(handler).toHaveBeenCalledWith({
        primitive: 42,
        // eslint-disable-next-line ts/no-unsafe-assignment
        date: expect.any(Date),
      })
    })
  })
})

describe('groups', () => {
  test('does not expose groupId on the public job handle', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      schema: type({ name: 'string' }),
      getGroupId: (input) => `group-for-${input.name}`,
      run: async () => {},
    })
    const job = await workflow.run({ name: 'A' })

    // @ts-expect-error groupId is worker-internal, not part of a producer's handle
    expect(job.groupId).toBeUndefined()
  })
})

/** Run `jobs` overlapping jobs through one worker and report the most ever in flight at once. */
async function peakInFlight(opts: {
  declared?: number
  requested?: number
  jobs: number
}): Promise<number> {
  let inFlight = 0
  let peak = 0
  const workflow = namespace().createWorkflow({
    id: randomUUID(),
    workerOptions: opts.declared === undefined ? undefined : { concurrency: opts.declared },
    run: async () => {
      peak = Math.max(peak, ++inFlight)
      await sleep(50)
      inFlight--
    },
  })
  const jobs = await Promise.all(
    Array.from({ length: opts.jobs }, async () => workflow.run(undefined)),
  )
  await workflow.work(opts.requested === undefined ? undefined : { concurrency: opts.requested })
  await Promise.all(jobs.map(async (job) => job.wait(5000)))
  return peak
}

describe('worker concurrency', () => {
  test("work() cannot raise the workflow's own concurrency", async () => {
    expect(await peakInFlight({ declared: 2, requested: 8, jobs: 6 })).toBe(2)
  })

  test('work() can still lower it', async () => {
    expect(await peakInFlight({ declared: 8, requested: 1, jobs: 4 })).toBe(1)
  })

  test('either one alone applies', async () => {
    expect(await peakInFlight({ declared: 3, jobs: 6 })).toBe(3)
    expect(await peakInFlight({ requested: 3, jobs: 6 })).toBe(3)
  })
})

test('job data that no longer matches the schema warns and fails without retrying', async () => {
  const logger = { ...console, warn: vi.fn() }
  const ns = new WorkflowNamespace({
    id: randomUUID(),
    redis: sharedRedis,
    logger,
    autoClose: false,
  })
  const wfId = randomUUID()

  // Enqueue under the old (permissive) schema, then work it under the new (stricter) one —
  // exactly the shape of a job left in the queue across a schema change.
  // A retry would re-read the same stored payload, so the budget must go unused.
  const oldVersion = ns.createWorkflow({
    id: wfId,
    run: async () => 'ok',
    jobOptions: { maxAttempts: 5 },
  })
  const job = await oldVersion.run({ name: 123 })

  const handler = vi.fn()
  const newVersion = ns.createWorkflow({
    id: wfId,
    schema: type({ name: 'string' }),
    run: handler,
  })
  await newVersion.work()

  await expect(job.wait(5000)).rejects.toThrow('Invalid workflow input')
  expect(handler).not.toHaveBeenCalled()
  expect(logger.warn).toHaveBeenCalledWith(
    expect.stringContaining('does not match the workflow schema'),
    expect.anything(),
  )
})

test('numeric priority — higher runs first', async () => {
  const handler = vi.fn()
  const workflow = namespace().createWorkflow({
    id: randomUUID(),
    schema: type({ priority: 'string' }),
    run: handler,
  })

  await workflow.run({ priority: 'normal' })
  await workflow.run({ priority: 'high' }, { priority: 1 })

  await workflow.work()

  await vi.waitFor(() => {
    expect(handler).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ input: { priority: 'high' } }),
    )
    expect(handler).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ input: { priority: 'normal' } }),
    )
  })
})

test('upsertSchedule validates input against the schema at registration', async () => {
  const workflow = namespace().createWorkflow({
    id: randomUUID(),
    schema: type({ name: 'string' }),
    run: async () => {},
  })

  await expect(
    // @ts-expect-error — invalid input must fail fast at registration
    workflow.upsertSchedule('bad', { pattern: '* * * * *', input: { name: 123 } }),
  ).rejects.toThrow('Invalid workflow input')

  await workflow.upsertSchedule('good', { pattern: '* * * * *', input: { name: 'A' } })
  const schedules = await workflow.getSchedules()
  expect(schedules).toHaveLength(1)
  expect(schedules[0]).toMatchObject({ scheduleId: 'good', pattern: '* * * * *' })
})

test('upsertSchedule is idempotent — re-upserting the same id replaces, never duplicates', async () => {
  const workflow = namespace().createWorkflow({
    id: randomUUID(),
    schema: type({ name: 'string' }),
    run: async () => {},
  })

  await workflow.upsertSchedule('nightly', {
    pattern: '* * * * *',
    input: { name: 'A' },
    tz: 'UTC',
  })
  await workflow.upsertSchedule('nightly', {
    pattern: '*/5 * * * *',
    input: { name: 'B' },
    tz: 'UTC',
  })

  // Keyed by (workflow, scheduleId): the second upsert overwrites in place — no duplicate.
  const schedules = await workflow.getSchedules()
  expect(schedules).toHaveLength(1)
  expect(schedules[0]).toMatchObject({ scheduleId: 'nightly', pattern: '*/5 * * * *' })
})

describe('durable sleep', () => {
  test('step.wait resumes on replay with the remaining time (not the full duration again)', async () => {
    const DURATION = 400
    const waitMs: number[] = []
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      workerOptions: { maxAttempts: 3 },
      run: async ({ step }) => {
        const start = Date.now()
        await step.wait('nap', DURATION)
        waitMs.push(Date.now() - start)
        // Force one retry so the SECOND run resumes an already-elapsed sleep.
        if (waitMs.length === 1) throw new Error('force retry')
        return 'done'
      },
    })
    await workflow.work({ backoff: () => 0 })
    const job = await workflow.run(undefined)

    await expect(job.wait()).resolves.toBe('done')
    // First attempt sleeps ~full duration; the resumed attempt computes remaining≈0 from the
    // persisted startedAt and returns near-instantly. A resume that re-slept the full duration
    // (ignoring startedAt) would push waitMs[1] up near DURATION.
    expect(waitMs[0]).toBeGreaterThanOrEqual(DURATION - 50)
    expect(waitMs[1]).toBeLessThan(DURATION / 2)
  })

  test('step.wait is signal-aware: losing the claim aborts the sleep and skips the commit', async () => {
    // Covers cancellation end-to-end: a stolen claim aborts ctx.signal, the signal-aware sleep
    // rejects, and the token-guarded commit never writes a result (no double-commit).
    const redis = await connect()
    const prefix = randomUUID()
    const wfId = randomUUID()
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      redis,
      prefix,
      logger: console,
      autoClose: false,
    })
    const started = makeGate()
    let aborted = false
    let committed = false
    const workflow = ns.createWorkflow({
      id: wfId,
      run: async ({ step }) => {
        started.open()
        try {
          await step.wait('long', 10_000) // signal-aware in-process sleep
        } catch {
          aborted = true // the AbortError from the lost claim propagated cooperatively
          throw new Error('aborted')
        }
        committed = true
        return 'done'
      },
    })
    // Short lock ⇒ heartbeat ~100ms detects the stolen token fast; long re-poll so no background
    // recovery re-reserves the claim mid-test.
    await workflow.work({ lockMs: 300, safetyTimeout: 30 })

    try {
      const job = await workflow.run(undefined)
      await started.wait()

      // Steal the claim: overwrite the lock with a foreign token (as recovery + re-reserve would).
      await redis.set(`${prefix}:${wfId}:j:${job.id}:lock`, 'foreign-token')

      await vi.waitFor(() => {
        expect(aborted).toBe(true) // heartbeat CAS returned 0 ⇒ ctx.signal aborted the sleep
      })
      expect(committed).toBe(false)
      // Token-safe: the aborted worker committed nothing (no result record written).
      expect(await redis.exists(`${prefix}:${wfId}:result:${job.id}`)).toBe(0)
    } finally {
      await ns.close()
    }
  })
})

test('priority is numeric — the old high|normal enum is gone', async () => {
  const workflow = namespace().createWorkflow({ id: randomUUID(), run: async () => {} })

  // A string priority is both a type error (enum retired) and rejected at runtime (RangeError).
  await expect(
    // @ts-expect-error priority is a raw number now; 'high'|'normal' was dropped
    workflow.run(undefined, { priority: 'high' }),
  ).rejects.toThrow()
})

describe('serialization', () => {
  test('superjson round-trips non-POJO input, step data, and output (Date/Map/BigInt/Set)', async () => {
    const payload = {
      when: new Date('2024-06-01T12:00:00.000Z'),
      tags: new Set(['a', 'b']),
      big: 42n,
    }
    let seenInput: typeof payload | undefined
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      schema: passthrough<typeof payload>(),
      run: async ({ input, step }) => {
        seenInput = input
        const stepOut = await step.do('s', () => new Map<string, bigint>([['x', 7n]]))
        return { echo: input, stepOut, total: input.big + stepOut.get('x')! }
      },
    })
    await workflow.work()
    const job = await workflow.run(payload)
    const result = await job.wait()

    expect(seenInput).toEqual(payload) // input round-trips through the payload
    expect(result.echo).toEqual(payload)
    expect(result.stepOut).toEqual(new Map([['x', 7n]])) // step-data hash round-trips
    expect(result.total).toBe(49n) // BigInt arithmetic proves it stayed a real bigint end-to-end
  })
})

test('retired surface is absent (keepCompleted / step retry / repeat / removeRepeatingJob)', async () => {
  const workflow = namespace().createWorkflow({
    id: randomUUID(),
    run: async ({ step }) =>
      // step.do takes no options bag — step-level `retry` was retired.
      // @ts-expect-error step-level retry option no longer exists
      step.do('s', () => 1, { retry: 3 }),
  })

  // The groupmq-era `removeRepeatingJob` patch is gone from the workflow surface entirely.
  expect((workflow as unknown as Record<string, unknown>).removeRepeatingJob).toBeUndefined()

  await workflow.work({
    // @ts-expect-error `keepCompleted` was retired (only `keepFailed` remains)
    keepCompleted: 100,
  })

  const job = await workflow.run(undefined, {
    // @ts-expect-error `repeat` was retired from run options (use upsertSchedule)
    repeat: '* * * * *',
  })
  // The retired options are ignored, not honored: the job still runs once and completes.
  await expect(job.wait()).resolves.toBe(1)
})

// ---------------------------------------------------------------------------
// Step memo prefetch + `done` publish payload (perf rework)
//
// Step data now rides along with the claim instead of being read per step, and the `done`
// publish carries the result record instead of a bare wake-up. Both are cross-process wire
// contracts, so they need to hold for a worker that never saw the earlier attempt and for a
// peer still running the older shape.
// ---------------------------------------------------------------------------

describe('step memo prefetch', () => {
  test('a retry picked up by a different worker still replays completed steps', async () => {
    // The memo is seeded from the step hash that `reserve` ships with the claim. Two workers on
    // separate connections means the retry can land on a worker whose heap never held this
    // job's step data — the prefetch is the only thing that can carry it across.
    // `namespace()` shares the suite-wide connection, so it must not be closed here.
    const ns = namespace()
    const stepA = vi.fn(async () => 'a')
    const stepB = vi.fn(async () => 'b')
    let attempts = 0

    const workflow = ns.createWorkflow({
      id: randomUUID(),
      run: async ({ step }) => {
        await step.do('a', stepA)
        attempts++
        if (attempts === 1) throw new Error('fail after a')
        await step.do('b', stepB)
        return 'done'
      },
    })

    await workflow.work({ concurrency: 5, maxAttempts: 5, backoff: () => 0 })
    await workflow.work({ concurrency: 5, maxAttempts: 5, backoff: () => 0 })

    const job = await workflow.run(undefined, { maxAttempts: 5 })
    await expect(job.wait(15_000)).resolves.toBe('done')

    // Step 'a' completed on attempt 1; the replay must return its cached result, not re-run it.
    expect(stepA).toHaveBeenCalledOnce()
    expect(stepB).toHaveBeenCalledOnce()
    expect(attempts).toBe(2)
  })
})

test('a bare "1" done publish (an older peer) still resolves via the result key', async () => {
  // The publish payload is a wire format shared with other processes. During a rolling deploy a
  // peer running the previous build publishes `"1"` with the record only in the result key —
  // that must still resolve, not be parsed as a result.
  const redis = await connect()
  const prefix = randomUUID()
  const wfId = randomUUID()
  const ns = new WorkflowNamespace({
    id: randomUUID(),
    redis,
    prefix,
    logger: console,
    autoClose: false,
  })
  const workflow = ns.createWorkflow({ id: wfId, run: async () => 'unused' })

  try {
    const job = await workflow.run(undefined)
    const waiting = job.wait(10_000)

    // Write the record the way the old code did, then ring the old doorbell.
    const resultKey = `${prefix}:${wfId}:result:${job.id}`
    const channel = `${prefix}:${wfId}:done:${job.id}`
    const record = { state: 'completed', value: stringify('legacy') }
    await redis.set(resultKey, JSON.stringify(record), 'EX', 60)
    const pump = setInterval(() => void redis.publish(channel, '1'), 20)

    await expect(waiting).resolves.toBe('legacy')
    clearInterval(pump)
  } finally {
    await ns.close()
  }
})

describe('rate limits', () => {
  test('shares every budget across workflows and admits bursts', async () => {
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      redis: await connect(),
      autoClose: false,
      rateLimiters: { api: { limit: 3, window: 240 }, search: { limit: 2, window: 100 } },
      queueOptions: { rateLimiters: ['api', 'search'] },
    })
    onTestFinished(async () => ns.close())
    const starts: number[] = []
    const workflows = ['a', 'b'].map(() =>
      ns.createWorkflow({
        id: randomUUID(),
        run: async () => {
          starts.push(Date.now())
        },
      }),
    )
    const jobs = await Promise.all(
      Array.from({ length: 12 }, async (_, i) => workflows[i % 2]!.run(undefined)),
    )
    await Promise.all(workflows.map(async (wf) => wf.work({ concurrency: 8 })))
    await Promise.all(jobs.map(async (job) => job.wait(4000)))
    expect(starts[1]! - starts[0]!).toBeLessThan(60)
    for (const { limit, window } of [
      { limit: 3, window: 240 },
      { limit: 2, window: 100 },
    ]) {
      for (let i = limit; i < starts.length; i++)
        expect(starts[i]! - starts[i - limit]!).toBeGreaterThanOrEqual(window - 10)
    }
  })

  test('counts retries and spaces starts with the local timer', async () => {
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      redis: await connect(),
      autoClose: false,
      rateLimiters: { api: { limit: 1, window: 100 } },
      queueOptions: { rateLimiters: ['api'] },
    })
    onTestFinished(async () => ns.close())
    const starts: number[] = []
    const wf = ns.createWorkflow({
      id: randomUUID(),
      run: async () => {
        starts.push(Date.now())
        if (starts.length < 5) throw new Error('retry')
        return 'done'
      },
      jobOptions: { maxAttempts: 5 },
    })
    const { job, events } = await wf.runAndWatch(undefined)
    const watching = collect(events)
    await wf.work({ backoff: () => 0 })
    await expect(job.wait(3000)).resolves.toBe('done')
    const recorded = await watching
    expect(
      recorded.filter((event) => event.type === 'started').map((event) => event.attempt),
    ).toEqual([1, 2, 3, 4, 5])
    for (let i = 1; i < starts.length; i++)
      expect(starts[i]! - starts[i - 1]!).toBeGreaterThanOrEqual(90)
    // BRPOP at Redis's default hz can add another 100 ms to each interval.
    expect(starts.at(-1)! - starts[0]!).toBeLessThan(650)
  })

  test('limited jobs stay waiting and keep priority, group position, and attempts', async () => {
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      redis: await connect(),
      autoClose: false,
      rateLimiters: { api: { limit: 1, window: 200 } },
      queueOptions: { rateLimiters: ['api'] },
    })
    onTestFinished(async () => ns.close())
    const order: string[] = []
    const wf = ns.createWorkflow({
      id: randomUUID(),
      schema: z.string(),
      run: async ({ input }) => {
        order.push(input)
      },
    })
    await wf.work({ concurrency: 8 })
    const first = await wf.run('first')
    await first.wait(1000)
    const low = await wf.runAndWatch('low')
    const high = await wf.runAndWatch('high', { priority: 2, groupId: 'group' })
    const next = await wf.runAndWatch('next', { priority: 2, groupId: 'group' })
    expect(await wf.getMetrics()).toEqual({ active: 0, waiting: 3, delayed: 0 })
    const events = await Promise.all([low, high, next].map(async (job) => collect(job.events)))
    expect(order).toEqual(['first', 'high', 'next', 'low'])
    for (const stream of events)
      expect(stream).toEqual([
        { type: 'started', attempt: 1 },
        { type: 'completed', output: undefined },
      ])
  })

  test('inherits the default list, replaces it, and opts out with an empty list', async () => {
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      redis: await connect(),
      autoClose: false,
      rateLimiters: { api: { limit: 1, window: 400 }, other: { limit: 1, window: 400 } },
      queueOptions: { rateLimiters: ['api'] },
    })
    onTestFinished(async () => ns.close())
    const inherited = ns.createWorkflow({ id: randomUUID(), run: async () => 'inherited' })
    const replaced = ns.createWorkflow({
      id: randomUUID(),
      queueOptions: { rateLimiters: ['other'] },
      run: async () => 'replaced',
    })
    const unlimited = ns.createWorkflow({
      id: randomUUID(),
      queueOptions: { rateLimiters: [] },
      run: async () => 'unlimited',
    })
    await Promise.all([inherited.work(), replaced.work(), unlimited.work()])
    const first = await inherited.run(undefined)
    await first.wait(1000)
    const waiting = await inherited.run(undefined)
    const replacement = await replaced.run(undefined)
    await expect(replacement.wait(200)).resolves.toBe('replaced')
    const optedOut = await unlimited.run(undefined)
    await expect(optedOut.wait(200)).resolves.toBe('unlimited')
    expect(await inherited.getMetrics()).toEqual({ active: 0, waiting: 1, delayed: 0 })
    expect(await ns.getRateLimiterMetrics()).toEqual({
      api: { starts: 1, pausedMs: 0 },
      other: { starts: 1, pausedMs: 0 },
    })
    await waiting.wait(1000)
  })

  test('a shorter-window process preserves starts recorded with a longer window', async () => {
    const id = randomUUID()
    const long = new WorkflowNamespace({
      id,
      redis: await connect(),
      autoClose: false,
      rateLimiters: { api: { limit: 2, window: 450 } },
      queueOptions: { rateLimiters: ['api'] },
    })
    const short = new WorkflowNamespace({
      id,
      redis: await connect(),
      autoClose: false,
      rateLimiters: { api: { limit: 2, window: 80 } },
      queueOptions: { rateLimiters: ['api'] },
    })
    onTestFinished(async () => {
      await Promise.all([long.close(), short.close()])
    })
    const a = long.createWorkflow({ id: randomUUID(), run: async () => Date.now() })
    const b = short.createWorkflow({ id: randomUUID(), run: async () => Date.now() })
    await a.work()
    await b.work()
    const firstJob = await a.run(undefined)
    const first = await firstJob.wait(1000)
    const shortJob = await b.run(undefined)
    await shortJob.wait(1000)
    await sleep(120)
    const nextShortJob = await b.run(undefined)
    await nextShortJob.wait(1000)
    expect(await long.getRateLimiterMetrics()).toEqual({ api: { starts: 2, pausedMs: 0 } })
    const nextJob = await a.run(undefined)
    const next = await nextJob.wait(1000)
    expect(next - first).toBeGreaterThanOrEqual(190)
    // The short writer must also preserve the long start when extending the log's TTL.
    expect(await long.getRateLimiterMetrics()).toEqual({ api: { starts: 2, pausedMs: 0 } })
  })

  test('pauses shared workflows, preserves memoized steps and reruns first without a failure', async () => {
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      redis: await connect(),
      autoClose: false,
      rateLimiters: { api: { limit: 20, window: 1000 } },
      queueOptions: { rateLimiters: ['api'] },
    })
    onTestFinished(async () => ns.close())
    const onFailed = vi.fn()
    const backoff = vi.fn(() => 0)
    const completed = vi.fn(() => 'cached')
    const order: string[] = []
    let calls = 0
    const running = makeGate()
    const finish = makeGate()
    const sibling = ns.createWorkflow({
      id: randomUUID(),
      run: async () => {
        running.open()
        await finish.wait()
        return 'finished'
      },
    })
    await sibling.work()
    const activeJob = await sibling.run(undefined)
    await running.wait()
    const wf = ns.createWorkflow({
      id: randomUUID(),
      schema: z.string(),
      workerOptions: { onFailed, backoff },
      run: async ({ input, step, rateLimit }) => {
        order.push(input)
        await step.do('cached', completed)
        await step.do('api', () => {
          if (++calls === 1) rateLimit(250)
        })
        return 'done'
      },
    })
    const first = await wf.runAndWatch('first', { groupId: 'group', maxAttempts: 1 })
    const second = await wf.run('second', { groupId: 'group' })
    const events = collect(first.events)
    await wf.work({ concurrency: 8 })
    await vi.waitFor(async () => {
      const { api } = await ns.getRateLimiterMetrics()
      expect(api.pausedMs).toBeGreaterThan(0)
    })
    expect(await wf.getMetrics()).toEqual({ active: 0, waiting: 2, delayed: 0 })
    finish.open()
    await expect(activeJob.wait(100)).resolves.toBe('finished')
    const blocked = await sibling.run(undefined)
    expect(await sibling.getMetrics()).toEqual({ active: 0, waiting: 1, delayed: 0 })
    await Promise.all([first.job.wait(2000), second.wait(2000), blocked.wait(2000)])
    expect(order).toEqual(['first', 'first', 'second'])
    expect(completed).toHaveBeenCalledTimes(2)
    expect(calls).toBe(3)
    expect(onFailed).not.toHaveBeenCalled()
    expect(backoff).not.toHaveBeenCalled()
    expect(await events).toEqual([
      { type: 'started', attempt: 1 },
      { type: 'started', attempt: 1 },
      { type: 'completed', output: 'done' },
    ])
    expect(await ns.getRateLimiterMetrics()).toEqual({ api: { starts: 5, pausedMs: 0 } })
  })

  test('a shorter second pause cannot shorten the first', async () => {
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      redis: await connect(),
      autoClose: false,
      rateLimiters: { api: { limit: 20, window: 1000 } },
      queueOptions: { rateLimiters: ['api'] },
    })
    onTestFinished(async () => ns.close())
    const bothStarted = makeGate()
    const shorterPause = makeGate()
    let calls = 0
    const starts: number[] = []
    const wf = ns.createWorkflow({
      id: randomUUID(),
      schema: z.number(),
      run: async ({ input, rateLimit }) => {
        starts.push(Date.now())
        if (++calls > 2) return
        if (calls === 2) bothStarted.open()
        await bothStarted.wait()
        if (input === 2) await shorterPause.wait()
        rateLimit(input === 1 ? 300 : 30)
      },
    })
    const jobs = [await wf.run(1), await wf.run(2)]
    await wf.work({ concurrency: 2 })
    await vi.waitFor(async () => {
      const { api } = await ns.getRateLimiterMetrics()
      expect(api.pausedMs).toBeGreaterThan(200)
    })
    shorterPause.open()
    await Promise.all(jobs.map(async (job) => job.wait(2000)))
    expect(starts[2]! - starts[0]!).toBeGreaterThanOrEqual(290)
  })

  test('without limiters, a pause affects only its workflow', async () => {
    const ns = new WorkflowNamespace({ id: randomUUID(), redis: await connect(), autoClose: false })
    onTestFinished(async () => ns.close())
    let calls = 0
    const wf = ns.createWorkflow({
      id: randomUUID(),
      run: async ({ rateLimit }) => {
        if (++calls === 1) rateLimit(200.2)
        return Date.now()
      },
    })
    const other = ns.createWorkflow({ id: randomUUID(), run: async () => Date.now() })
    await wf.work()
    await other.work()
    const started = Date.now()
    const job = await wf.run(undefined)
    await vi.waitFor(() => expect(calls).toBe(1))
    const otherJob = await other.run(undefined)
    expect((await otherJob.wait(100)) - started).toBeLessThan(150)
    expect((await job.wait(1000)) - started).toBeGreaterThanOrEqual(200)
    expect(await ns.getRateLimiterMetrics()).toEqual({})
  })

  test.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY])(
    'rejects an invalid pause of %s through normal failure handling',
    async (ms) => {
      const ns = new WorkflowNamespace({
        id: randomUUID(),
        redis: await connect(),
        autoClose: false,
      })
      onTestFinished(async () => ns.close())
      const onFailed = vi.fn()
      const wf = ns.createWorkflow({
        id: randomUUID(),
        run: async ({ rateLimit }) => rateLimit(ms),
      })
      await wf.work({ onFailed })
      const job = await wf.run(undefined)
      await expect(job.wait(1000)).rejects.toThrow('positive finite')
      expect(onFailed).toHaveBeenCalledOnce()
    },
  )

  test('writes a pause after the worker has lost its claim', async () => {
    const redis = await connect()
    const prefix = randomUUID()
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      redis,
      prefix,
      autoClose: false,
      rateLimiters: { api: { limit: 20, window: 1000 } },
      queueOptions: { rateLimiters: ['api'] },
    })
    onTestFinished(async () => ns.close())
    const started = makeGate()
    const onFailed = vi.fn()
    const wf = ns.createWorkflow({
      id: randomUUID(),
      run: async ({ step, rateLimit }) => {
        started.open()
        try {
          await step.wait('lost-claim', 10_000)
        } catch {
          rateLimit(300)
        }
      },
    })
    await wf.work({ lockMs: 90, safetyTimeout: 30, onFailed })
    const job = await wf.run(undefined)
    await started.wait()
    await redis.set(`${prefix}:${wf.id}:j:${job.id}:lock`, 'new-owner')
    await vi.waitFor(async () => {
      const { api } = await ns.getRateLimiterMetrics()
      expect(api.pausedMs).toBeGreaterThan(0)
    })
    expect(await wf.getMetrics()).toEqual({ active: 1, waiting: 0, delayed: 0 })
    expect(onFailed).not.toHaveBeenCalled()
  })

  test('a stalled re-claim counts as another start', async () => {
    const redis = await connect()
    const prefix = randomUUID()
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      redis,
      prefix,
      autoClose: false,
      rateLimiters: { api: { limit: 1, window: 200 } },
      queueOptions: { rateLimiters: ['api'] },
    })
    onTestFinished(async () => ns.close())
    const started = makeGate()
    const starts: number[] = []
    const wf = ns.createWorkflow({
      id: randomUUID(),
      run: async ({ step }) => {
        starts.push(Date.now())
        if (starts.length === 1) {
          started.open()
          await step.wait('until-recovery', 10_000)
        }
      },
    })
    const { job, events } = await wf.runAndWatch(undefined)
    const watching = collect(events)
    await wf.work({ lockMs: 90, stalledInterval: 25, safetyTimeout: 0.05 })
    await started.wait()
    await redis.del(`${prefix}:${wf.id}:j:${job.id}:lock`)
    await job.wait(2000)
    expect(starts).toHaveLength(2)
    expect(starts[1]! - starts[0]!).toBeGreaterThanOrEqual(190)
    expect(await watching).toEqual([
      { type: 'started', attempt: 1 },
      { type: 'started', attempt: 1 },
      { type: 'completed', output: undefined },
    ])
  })

  test('a wrapped pause error fails normally', async () => {
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      redis: await connect(),
      autoClose: false,
      rateLimiters: { api: { limit: 10, window: 1000 } },
      queueOptions: { rateLimiters: ['api'] },
    })
    onTestFinished(async () => ns.close())
    const onFailed = vi.fn()
    const wf = ns.createWorkflow({
      id: randomUUID(),
      run: async ({ rateLimit }) => {
        try {
          rateLimit(200)
        } catch (err) {
          throw new Error('wrapped', { cause: err })
        }
      },
    })
    await wf.work({ onFailed })
    const job = await wf.run(undefined)
    await expect(job.wait(1000)).rejects.toThrow('wrapped')
    expect(onFailed).toHaveBeenCalledOnce()
    expect(await ns.getRateLimiterMetrics()).toEqual({ api: { starts: 1, pausedMs: 0 } })
  })

  test('reports pause storage errors without failing the job', async () => {
    const ns = new WorkflowNamespace({ id: randomUUID(), redis: await connect(), autoClose: false })
    onTestFinished(async () => ns.close())
    const storageError = new Error('pause storage failed')
    const internal = await ns.getNamespace()
    const pause = vi.spyOn(internal.redis, 'rateLimit').mockRejectedValue(storageError)
    const onError = vi.fn()
    const onFailed = vi.fn()
    const backoff = vi.fn(() => 0)
    const wf = ns.createWorkflow({ id: randomUUID(), run: async ({ rateLimit }) => rateLimit(100) })
    await wf.work({ onError, onFailed, backoff })
    await wf.run(undefined)
    await vi.waitFor(() => expect(onError).toHaveBeenCalledWith(storageError))
    expect(onFailed).not.toHaveBeenCalled()
    expect(backoff).not.toHaveBeenCalled()
    pause.mockRestore()
  })

  test.each([0, -1, 0.5, Number.NaN, Number.POSITIVE_INFINITY])(
    'rejects invalid budgets of %s in the constructor',
    (value) => {
      for (const budget of [
        { limit: value, window: 100 },
        { limit: 1, window: value },
      ])
        expect(
          () => new WorkflowNamespace({ id: randomUUID(), rateLimiters: { api: budget } }),
        ).toThrow('positive integer')
    },
  )

  test('infers valid limiter names and the resolved workflow list', () => {
    const ns = new WorkflowNamespace({
      id: randomUUID(),
      rateLimiters: { api: { limit: 1, window: 100 }, other: { limit: 2, window: 100 } },
      queueOptions: { rateLimiters: ['api'] },
    })
    const invalid = new WorkflowNamespace({
      id: randomUUID(),
      rateLimiters: { api: { limit: 1, window: 100 } },
      // @ts-expect-error a namespace default must name a declared limiter
      queueOptions: { rateLimiters: ['missing'] },
    })
    void invalid
    ns.createWorkflow({
      id: 'invalid',
      // @ts-expect-error a workflow must name a declared limiter
      queueOptions: { rateLimiters: ['missing'] },
      run: async () => {},
    })
    ns.createWorkflow({
      id: 'default',
      run: async ({ rateLimit }) => {
        const check = () => {
          rateLimit(100)
          rateLimit(100, 'api')
          // @ts-expect-error not in the inherited list
          rateLimit(100, 'other')
        }
        void check
      },
    })
    ns.createWorkflow({
      id: 'several',
      queueOptions: { rateLimiters: ['api', 'other'] },
      run: async ({ rateLimit }) => {
        const check = () => {
          rateLimit(100, 'api')
          rateLimit(100, 'other')
          // @ts-expect-error several limiters require a name
          rateLimit(100)
          // @ts-expect-error unknown limiter
          rateLimit(100, 'missing')
        }
        void check
      },
    })
    ns.createWorkflow({
      id: 'replacement',
      queueOptions: { rateLimiters: ['other'] },
      run: async ({ rateLimit }) => {
        const check = () => {
          rateLimit(100)
          rateLimit(100, 'other')
          // @ts-expect-error the workflow replaced the default
          rateLimit(100, 'api')
        }
        void check
      },
    })
    ns.createWorkflow({
      id: 'none',
      queueOptions: { rateLimiters: [] },
      run: async ({ rateLimit, step }) => {
        const check = () => {
          rateLimit(100)
          // @ts-expect-error a workflow with no limiters takes no name
          rateLimit(100, 'api')
          // @ts-expect-error rateLimit exists only on the run context
          void step.rateLimit
        }
        void check
      },
    })
    new WorkflowNamespace({ id: randomUUID() }).createWorkflow({
      id: 'undeclared',
      run: async ({ rateLimit }) => {
        const check = () => {
          rateLimit(100)
          // @ts-expect-error no declared or selected limiter
          rateLimit(100, 'missing')
        }
        void check
      },
    })
  })
})

test('observes each limiter once per meter and traces pauses without errors', async () => {
  const spanExporter = new InMemorySpanExporter()
  const tracerProvider = new BasicTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(spanExporter)],
  })
  trace.setGlobalTracerProvider(tracerProvider)
  const reader = new PeriodicExportingMetricReader({
    exporter: new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE),
    exportIntervalMillis: 60_000,
  })
  const provider = new MeterProvider({ readers: [reader] })
  const meter = provider.getMeter('rate-limits')
  const register = vi.spyOn(meter, 'addBatchObservableCallback')
  const ns = new WorkflowNamespace({
    id: randomUUID(),
    redis: await connect(),
    autoClose: false,
    rateLimiters: { api: { limit: 20, window: 400 }, unused: { limit: 1, window: 400 } },
    queueOptions: { rateLimiters: ['api'] },
    workerOptions: { metrics: { meter, prefix: 'test' } },
  })
  onTestFinished(async () => {
    await provider.shutdown()
    await ns.close()
    await tracerProvider.shutdown()
    trace.disable()
  })
  let calls = 0
  const wf = ns.createWorkflow({
    id: randomUUID(),
    run: async ({ step, rateLimit }) => {
      await step.do('request', () => {
        if (++calls === 1) rateLimit(200)
      })
    },
  })
  const other = ns.createWorkflow({ id: randomUUID(), run: async () => 'ok' })
  let localCalls = 0
  const local = ns.createWorkflow({
    id: randomUUID(),
    queueOptions: { rateLimiters: [] },
    run: async ({ rateLimit }) => {
      if (++localCalls === 1) rateLimit(50)
    },
  })
  await wf.work()
  await other.work()
  // Two workflow callbacks, one namespace callback for their shared meter.
  expect(register).toHaveBeenCalledTimes(3)
  const job = await wf.run(undefined)
  await vi.waitFor(async () => {
    const { api } = await ns.getRateLimiterMetrics()
    expect(api.pausedMs).toBeGreaterThan(0)
  })
  const { resourceMetrics } = await reader.collect()
  const metrics = resourceMetrics.scopeMetrics.flatMap((scope) => scope.metrics)
  const starts = metrics.find((metric) => metric.descriptor.name === 'test_rate_limiter_starts')!
  const paused = metrics.find((metric) => metric.descriptor.name === 'test_rate_limiter_paused_ms')!
  expect(starts.dataPoints.map((point) => [point.attributes.rate_limiter, point.value])).toEqual([
    ['api', 1],
    ['unused', 0],
  ])
  expect(paused.dataPoints).toHaveLength(2)
  expect(
    paused.dataPoints.find((point) => point.attributes.rate_limiter === 'api')!.value,
  ).toBeGreaterThan(0)
  await job.wait(2000)
  await local.work()
  const localJob = await local.run(undefined)
  await localJob.wait(1000)
  await tracerProvider.forceFlush()
  const spans = spanExporter.getFinishedSpans()
  const pausedSpan = spans.find(
    (span) => span.name === `workflow-worker/${wf.id}` && span.status.code === SpanStatusCode.UNSET,
  )!
  expect(pausedSpan.events).toEqual([
    expect.objectContaining({
      name: 'workflow.rate_limit',
      attributes: { 'workflow.rate_limiter': 'api', 'workflow.rate_limit.pause_ms': 200 },
    }),
  ])
  expect(pausedSpan.attributes).not.toHaveProperty('workflow.rate_limiter')
  const pausedStep = spans.find(
    (span) => span.name.endsWith('/step/request') && span.status.code === SpanStatusCode.UNSET,
  )!
  expect(pausedStep.events).toEqual([])
  const localSpan = spans.find(
    (span) =>
      span.name === `workflow-worker/${local.id}` && span.status.code === SpanStatusCode.UNSET,
  )!
  expect(localSpan.events[0]?.attributes).toEqual({ 'workflow.rate_limit.pause_ms': 50 })
  await sleep(450)
  expect(await ns.getRateLimiterMetrics()).toEqual({
    api: { starts: 0, pausedMs: 0 },
    unused: { starts: 0, pausedMs: 0 },
  })
})
