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
    await step.do('count', async () => counter.incr(`${prefix}:runs`))
    return 'ok'
  },
})

await workflow.work({ concurrency: Number(concurrency), backoff: () => 0 })

process.send?.('ready')
