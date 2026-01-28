# @falcondev-oss/workflow

Simple type-safe queue worker with durable execution using Redis.

## Installation

```bash
npm install @falcondev-oss/workflow
```

## Usage

```ts
const workflow = new Workflow({
  id: 'example-workflow',
  input: z.object({
    timezone: z.string().default('UTC'),
    name: z.string(),
  }),
  async run({ input, step }) {
    await step.do('send welcome', () => {
      console.log(`Welcome, ${input.name}! Timezone: ${input.timezone}`)
    })

    await step.wait('wait a lil', 60_000)

    const isEngaged = await step.do('check engagement', () => {
      return Math.random() > 0.5
    })

    if (!isEngaged) return { engagementLevel: 'low' }

    await step.do('send tips', () => {
      console.log(`Here are some tips to get started, ${input.name}!`)
    })

    await step.wait('wait feedback', 3000)

    await step.do('send survey', () => {
      console.log(`Hi ${input.name}, please take our survey!`)
    })

    return {
      engagementLevel: 'high',
    }
  },
})

// Start worker
await workflow.work()

// Run workflow
const job = await workflow.run({
  name: 'John Doe',
  timezone: 'America/New_York',
})

// Wait for completion
const result = await job.wait()
console.log(result.engagementLevel)
```

## Inspiration

- https://x.com/imsh4yy/status/1984073526605967785?s=46
