/**
 * A worker in its own OS process, for `tests/multi-process.test.ts`.
 *
 * Config comes in on argv. Every job bumps a shared Redis counter, so the parent can prove
 * exactly-once execution across processes without any IPC bookkeeping. The parent kills us when
 * the assertion is done.
 */

import type { RateLimiterBudget } from '../../src/queue/types'
import process from 'node:process'
import { createRedis, WorkflowNamespace } from '../../src'

const [port, prefix, nsId, wfId, concurrency, budget] = process.argv.slice(2)
const host = 'localhost'
const rateLimit = JSON.parse(budget ?? 'null') as RateLimiterBudget | null

const counter = await createRedis({ host, port: Number(port) })
const ns = new WorkflowNamespace({
  id: nsId!,
  prefix: prefix!,
  redis: await createRedis({ host, port: Number(port) }),
  autoClose: false,
  rateLimiters: rateLimit ? { api: rateLimit } : {},
  queueOptions: { rateLimiters: rateLimit ? ['api'] : [] },
})

const workflow = ns.createWorkflow({
  id: wfId!,
  run: async ({ step }) => {
    if (rateLimit) await counter.rpush(`${prefix}:starts`, Date.now())
    // Inside a step: a replay would return the cached result without re-incrementing, so the
    // counter measures real executions, not attempts.
    await step.do('count', async () => counter.incr(`${prefix}:runs`))
    return 'ok'
  },
})

await workflow.work({ concurrency: Number(concurrency), backoff: () => 0 })

process.send?.('ready')
