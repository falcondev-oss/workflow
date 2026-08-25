import type { StandardSchemaV1 } from '@standard-schema/spec'
import { randomUUID } from 'node:crypto'
import { sleep } from '@antfu/utils'
import { type } from 'arktype'
import { stringify } from 'superjson'
import { beforeAll, describe, expect, test, vi } from 'vitest'
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
