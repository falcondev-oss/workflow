import type { StandardSchemaV1 } from '@standard-schema/spec'
import { randomUUID } from 'node:crypto'
import { sleep } from '@antfu/utils'
import { type } from 'arktype'
import { beforeAll, describe, expect, test, vi } from 'vitest'
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
  test('random id if not specified', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      schema: type({ name: 'string' }),
      run: async () => {},
    })
    await workflow.work()
    const job = await workflow.run({ name: 'A' })

    expect(job.groupId).toMatch(/^[0-9a-f-]{36}$/i)
  })

  test('uses specified groupId getter', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      schema: type({ name: 'string' }),
      getGroupId: (input) => `group-for-${input.name}`,
      run: async () => {},
    })
    await workflow.work()
    const job = await workflow.run({ name: 'A' })

    expect(job.groupId).toBe('group-for-A')
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
