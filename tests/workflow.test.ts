import { randomUUID } from 'node:crypto'
import { sleep } from '@antfu/utils'
import { type } from 'arktype'
import { beforeAll, describe, expect, test, vi } from 'vitest'
import { createRedis, Settings, WorkflowNamespace } from '../src'

beforeAll(() => {
  Settings.logger = console
  Settings.defaultConnection = async () =>
    createRedis({
      host: 'localhost',
      port: Number(process.env.REDIS_PORT),
    })
})

/** Mint a fresh namespace so each test is key-isolated by its random workflow/namespace ids. */
function namespace() {
  return new WorkflowNamespace({ id: randomUUID() })
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

  test('throws the workflow failure', async () => {
    const workflow = namespace().createWorkflow({
      id: randomUUID(),
      run: async () => {
        throw new Error('boom')
      },
    })
    await workflow.work({ backoff: () => 0 })
    const job = await workflow.run(undefined)

    await expect(job.wait()).rejects.toThrow('boom')
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
