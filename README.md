# @falcondev-oss/workflow

Durable, type-safe queue workers on Redis. Workflows are plain async functions whose `step`s are
memoized in Redis, so a retried job replays completed steps instead of re-running them.

## Installation

```bash
npm install @falcondev-oss/workflow
```

Requires Redis (any version with Lua scripting) and Node 24.

## Usage

Workflows live in a `WorkflowNamespace`, which owns the Redis connection, the cross-workflow
concurrency cap, and the shared option defaults.

```ts
import { createRedis, WorkflowNamespace } from '@falcondev-oss/workflow'
import { z } from 'zod'

const namespace = new WorkflowNamespace({
  id: 'my-app',
  redis: await createRedis({ url: process.env.REDIS_URL }),
  logger: console,
})

const workflow = namespace.createWorkflow({
  id: 'example-workflow',
  schema: z.object({
    timezone: z.string().default('UTC'),
    name: z.string(),
  }),
  async run({ input, step }) {
    await step.do('send welcome', () => {
      console.log(`Welcome, ${input.name}! Timezone: ${input.timezone}`)
    })

    await step.wait('wait a lil', 60_000)

    const isEngaged = await step.do('check engagement', () => Math.random() > 0.5)
    if (!isEngaged) return { engagementLevel: 'low' }

    await step.do('send tips', () => {
      console.log(`Here are some tips to get started, ${input.name}!`)
    })

    return { engagementLevel: 'high' }
  },
})

// Start a worker for this process
await workflow.work()

// Enqueue a run
const job = await workflow.run({ name: 'John Doe', timezone: 'America/New_York' })

// Wait for completion (works from a pure producer too — no worker needed)
const result = await job.wait()
console.log(result.engagementLevel)
```

### Watching jobs

Declare a Standard Schema for progress, then emit its input type from any step. Watchers receive
the validated output type on the same stream as lifecycle and terminal events.

```ts
const exportPdf = namespace.createWorkflow({
  id: 'export-pdf',
  schema: z.object({ reportId: z.string() }),
  progressSchema: z.object({
    label: z.string(),
    done: z.number(),
    total: z.number(),
  }),
  async run({ input, step }) {
    const rows = await step.do('fetch rows', async ({ step: nestedStep }) => {
      await nestedStep.progress({ label: 'Fetching rows', done: 0, total: 0 })
      return loadRows(input.reportId)
    })
    await step.progress({ label: 'Rendering', done: 0, total: rows.length })
    return renderPdf(rows)
  },
})

const { job, events } = await exportPdf.runAndWatch({ reportId: '1' })
for await (const event of events) {
  if (event.type === 'progress') console.log(event.data.label)
  if (event.type === 'completed') console.log(event.output)
}
```

Events published before `watch()` attaches are lost. Lifecycle and progress events are not
persisted, and the watcher reads no progress snapshot. Use `runAndWatch()` when the first event
matters because it subscribes before enqueueing. To attach from another request or process, build
a handle from the known id:

```ts
const job = await exportPdf.getJob(jobId)
const events = await job.watch({ signal: request.signal })
```

`watch()` subscribes before it resolves and buffers until iteration starts. Breaking out of the
loop unsubscribes. A retry emits another `started` event, while `failed` is terminal. A completed
job still yields its stored terminal result until the configured result TTL expires.

The library does not derive a percentage, ETA, or step count because workflows have no declared
step list. If a workflow knows a total, include it in its progress payload as above. Without a
`progressSchema`, `step.progress()` is a type error and the `progress` event arm is absent.

### Steps

- `step.do(name, fn)` — run once, memoize the result. Replayed from Redis on a retry.
- `step.wait(name, ms)` — durable sleep; remaining time is computed from the persisted start.
- `step.waitUntil(name, date)` — the same, to an absolute time.

Steps nest: the callback receives its own `step` scoped under the parent's name.

### Scheduling

```ts
await workflow.run(input) // now
await workflow.runIn(input, 60_000) // in 60s
await workflow.runAt(input, new Date('2030-01-01')) // at a time

// Cron, keyed by (workflow, scheduleId) — upserting the same id replaces in place
await workflow.upsertSchedule('nightly', {
  pattern: '0 3 * * *',
  input,
  tz: 'Europe/Berlin',
})
await workflow.getSchedules()
await workflow.removeSchedule('nightly')
```

### Ordering, priority and concurrency

Jobs sharing a `groupId` run one at a time, in enqueue order. Everything else runs in parallel up
to the concurrency caps.

```ts
namespace.createWorkflow({
  id: 'per-user',
  schema: z.object({ userId: z.string() }),
  getGroupId: (input) => input.userId, // serialize per user
  queueOptions: { concurrency: 10, groupConcurrency: 1 },
  workerOptions: { concurrency: 4, maxAttempts: 3 },
  jobOptions: { priority: 1 }, // 0…2^21-1, higher runs first
  run: async () => {},
})
```

### Options

`WorkflowNamespace` options are shared defaults — each is shallow-merged under the matching
per-workflow override.

| Option          | Default    | Description                                             |
| --------------- | ---------- | ------------------------------------------------------- |
| `id`            | —          | Namespace id; scopes the cross-workflow concurrency cap |
| `redis`         | new client | Shared connection, owned by the namespace               |
| `prefix`        | `wf`       | Global key prefix                                       |
| `concurrency`   | unlimited  | Ceiling across all workflows in the namespace           |
| `logger`        | none       | Inherited by every workflow, queue and worker           |
| `autoClose`     | `true`     | Close (drain workers, disconnect) on `SIGINT`/`SIGTERM` |
| `queueOptions`  | —          | Defaults for every workflow's queue                     |
| `workerOptions` | —          | Defaults for every workflow's worker                    |
| `jobOptions`    | —          | Defaults for every enqueued job                         |

Shutdown is handled at the namespace: workers drain in-flight jobs, then the connections close.
Set `autoClose: false` and call `await namespace.close()` yourself to own it.

### Failures and retries

A throwing handler is retried up to `maxAttempts` with an exponential backoff (`expBackoff()`),
then dead-lettered. Throw `NonRecoverableError` to dead-letter immediately, skipping the
remaining budget — the library does this itself when a job's stored payload no longer validates
against the workflow `schema` (a job enqueued before a schema change), since a retry would only
re-read the same payload.

### Metrics

`workflow.getMetrics()` returns point-in-time `{ active, waiting, delayed }` depths. Pass an
OpenTelemetry meter to export them as gauges:

```ts
const namespace = new WorkflowNamespace({
  id: 'my-app',
  workerOptions: { metrics: { meter, prefix: 'my_app' } },
})
```

Spans are emitted for producers, workers and each step via the global OpenTelemetry tracer.

## Inspiration

- https://x.com/imsh4yy/status/1984073526605967785?s=46

