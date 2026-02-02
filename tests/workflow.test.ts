import { randomUUID } from 'node:crypto'
import { type } from 'arktype'
import { beforeAll, describe, expect, test, vi } from 'vitest'
import { createRedis, Settings, Workflow } from '../src'

beforeAll(() => {
  Settings.logger = console
  Settings.defaultConnection = async () =>
    createRedis({
      host: 'localhost',
      port: Number(process.env.REDIS_PORT),
    })
})

describe('input', () => {
  test('primitive', async () => {
    const handler = vi.fn()
    const workflow = new Workflow({
      id: randomUUID(),
      schema: type({
        name: 'string',
      }),
      run: handler,
    })
    await workflow.work()
    await workflow.run({ name: 'A' })

    await vi.waitFor(() =>
      expect(handler).toHaveBeenCalledExactlyOnceWith(
        expect.objectContaining({
          input: { name: 'A' },
        }),
      ),
    )
  })
  test('non-pojos', async () => {
    const handler = vi.fn()
    const workflow = new Workflow({
      id: randomUUID(),
      schema: type({
        date: 'Date',
      }),
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

describe('step', () => {
  test('only runs once', async () => {
    const stepHandler1 = vi.fn()
    const stepHandler2 = vi.fn()
    const stepHandler3 = vi.fn()
    const handler = vi.fn()
    const workflow = new Workflow({
      id: randomUUID(),
      queueOptions: {
        maxAttempts: 10,
      },
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
    await workflow.work({
      backoff: () => 0,
    })

    await workflow.run(undefined)

    await vi.waitFor(() => {
      expect(stepHandler1).toHaveBeenCalledOnce()
      expect(stepHandler2).toHaveBeenCalledOnce()
      expect(stepHandler3).toHaveBeenCalledOnce()
      expect(handler).toHaveBeenCalledTimes(10)
    })
  })
  test('caches output', async () => {
    const stepHandler = vi.fn(() => ({
      primitive: 42,
      date: new Date(),
    }))
    const handler = vi.fn()
    const workflow = new Workflow({
      id: randomUUID(),
      run: async ({ step }) => {
        const result = await step.do('test-step', stepHandler)
        await handler(result)
        throw new Error('error')
      },
    })
    await workflow.work({
      backoff: () => 0,
    })

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
    const workflow = new Workflow({
      id: randomUUID(),
      schema: type({
        name: 'string',
      }),
      run: async () => {},
    })
    await workflow.work()
    const job = await workflow.run({ name: 'A' })

    expect(job.groupId).toMatch(/^[0-9a-f-]{36}$/i)
  })

  test('uses specified groupId getter', async () => {
    const workflow = new Workflow({
      id: randomUUID(),
      schema: type({
        name: 'string',
      }),
      getGroupId: (input) => {
        return `group-for-${input.name}`
      },
      run: async () => {},
    })
    await workflow.work()
    const job = await workflow.run({ name: 'A' })

    expect(job.groupId).toBe('group-for-A')
  })
})

test('priority', async () => {
  const handler = vi.fn()
  const workflow = new Workflow({
    id: randomUUID(),
    schema: type({
      priority: 'string',
    }),
    run: handler,
  })

  await workflow.run({ priority: 'normal' })
  await workflow.run(
    { priority: 'high' },
    {
      priority: 'high',
    },
  )

  await workflow.work()

  await vi.waitFor(() => {
    expect(handler).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        input: { priority: 'high' },
      }),
    )
    expect(handler).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        input: { priority: 'normal' },
      }),
    )
  })
})
