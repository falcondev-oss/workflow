import type { Span } from '@opentelemetry/api'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type {
  ConnectionOptions,
  JobSchedulerTemplateOptions,
  JobsOptions,
  QueueEventsOptions,
  QueueOptions,
  RepeatOptions,
  WorkerOptions,
} from 'bullmq'
import type { Except, IsUnknown, SetOptional } from 'type-fest'
import type { Serialized } from './serializer'
import type { WorkflowJobInternal, WorkflowQueueInternal } from './types'
import { context, propagation, ROOT_CONTEXT, SpanKind } from '@opentelemetry/api'
import { Queue, QueueEvents, Worker } from 'bullmq'
import { asyncExitHook } from 'exit-hook'
import { WorkflowInputError } from './errors'
import { WorkflowJob } from './job'
import { deserialize, serialize } from './serializer'
import { defaultRedisConnection, Settings } from './settings'
import { WorkflowStep } from './step'
import { runWithTracing } from './tracer'

export interface WorkflowOptions<RunInput, Input, Output> {
  id: string
  schema?: StandardSchemaV1<RunInput, Input>
  run: (ctx: WorkflowRunContext<Input>) => Promise<Output>
  queueOptions?: SetOptional<QueueOptions, 'connection'> & {
    globalConcurrency?: number
    globalRateLimit?: {
      max: number
      duration: number
    }
  }
  workerOptions?: SetOptional<WorkerOptions, 'connection'>
  queueEventsOptions?: SetOptional<QueueEventsOptions, 'connection'>
  connection?: ConnectionOptions
}

export class Workflow<RunInput, Input, Output> {
  id
  private opts
  private queue?: WorkflowQueueInternal<Input, Output>
  private queueEvents?: QueueEvents

  constructor(opts: WorkflowOptions<RunInput, Input, Output>) {
    this.id = opts.id
    this.opts = opts
  }

  async work(opts?: Omit<SetOptional<WorkerOptions, 'connection'>, 'autorun'>) {
    const queue = await this.getOrCreateQueue()

    const worker = new Worker<WorkflowJobInternal<Input, Output>['data'], Serialized<Output>>(
      this.opts.id,
      async (job) => {
        Settings.logger?.info?.(`Processing workflow job ${job.id} of workflow ${this.opts.id}`)
        const jobId = job.id
        if (!jobId) throw new Error('Job ID is missing')

        const deserializedData = deserialize(job.data)
        const parsedData =
          this.opts.schema && (await this.opts.schema['~standard'].validate(deserializedData.input))
        if (parsedData?.issues)
          throw new WorkflowInputError('Invalid workflow input', parsedData.issues)

        return runWithTracing(
          `workflow-worker/${this.opts.id}`,
          {
            attributes: {
              'workflow.id': this.opts.id,
              'workflow.job_id': jobId,
            },
            kind: SpanKind.CONSUMER,
          },
          async (span) => {
            const start = performance.now()
            const result = await this.opts.run({
              // eslint-disable-next-line ts/no-unsafe-assignment
              input: parsedData?.value as any,
              step: new WorkflowStep({
                queue,
                workflowJobId: jobId,
                workflowId: this.opts.id,
              }),
              span,
            })

            const end = performance.now()

            Settings.logger?.success?.(
              `Completed workflow job ${job.id} of workflow ${this.opts.id} in ${(end - start).toFixed(2)} ms`,
            )
            return serialize(result)
          },
          propagation.extract(ROOT_CONTEXT, deserializedData.tracingHeaders),
        )
      },
      {
        connection: this.opts.connection ?? (await defaultRedisConnection()),
        prefix: Settings.defaultPrefix,
        ...this.opts.workerOptions,
        ...opts,
      },
    )
    await worker.waitUntilReady()
    Settings.logger?.info?.(`Worker started for workflow ${this.opts.id}`)

    asyncExitHook(
      async (signal) => {
        Settings.logger?.info?.(
          `Received ${signal}, shutting down worker for workflow ${this.opts.id}...`,
        )
        await worker.close()
      },
      { wait: 10_000 },
    )

    return this
  }

  async run(input: RunInput, opts?: JobsOptions) {
    const parsedInput = this.opts.schema && (await this.opts.schema['~standard'].validate(input))
    if (parsedInput?.issues)
      throw new WorkflowInputError('Invalid workflow input', parsedInput.issues)

    const queue = await this.getOrCreateQueue()
    return runWithTracing(
      `workflow-producer/${this.opts.id}`,
      {
        attributes: {
          'workflow.id': this.opts.id,
        },
        kind: SpanKind.PRODUCER,
      },
      async () => {
        const tracingHeaders = {}
        propagation.inject(context.active(), tracingHeaders)

        const job = await queue.add(
          'workflow-job',
          serialize({
            input: parsedInput?.value,
            stepData: {},
            tracingHeaders,
          }),
          opts,
        )

        return new WorkflowJob<Output>({
          job,
          queueEvents: await this.getOrCreateQueueEvents(),
        })
      },
    )
  }

  async runIn(input: RunInput, delayMs: number, opts?: Except<JobsOptions, 'delay'>) {
    return this.run(input, {
      delay: delayMs,
      ...opts,
    })
  }

  async runAt(input: RunInput, date: Date, opts?: Except<JobsOptions, 'delay'>) {
    const now = Date.now()
    const dateTime = date.getTime()

    return dateTime < now
      ? this.run(input, opts)
      : this.runIn(input, date.getTime() - Date.now(), opts)
  }

  private async runSchedule(
    schedulerId: string,
    repeatOpts: Omit<RepeatOptions, 'key'>,
    input: RunInput,
    opts?: JobSchedulerTemplateOptions,
  ) {
    const parsedInput = this.opts.schema && (await this.opts.schema['~standard'].validate(input))
    if (parsedInput?.issues)
      throw new WorkflowInputError('Invalid workflow input', parsedInput.issues)

    const queue = await this.getOrCreateQueue()
    await queue.upsertJobScheduler(schedulerId, repeatOpts, {
      name: 'workflow-job',
      data: serialize({
        input: parsedInput?.value,
        stepData: {},
        tracingHeaders: {},
      }),
      opts,
    })
  }

  async runCron(
    schedulerId: string,
    cron: string,
    input: RunInput,
    opts?: JobSchedulerTemplateOptions,
  ) {
    return this.runSchedule(
      schedulerId,
      {
        pattern: cron,
      },
      input,
      opts,
    )
  }

  async runEvery(
    schedulerId: string,
    everyMs: number,
    input: RunInput,
    opts?: JobSchedulerTemplateOptions,
  ) {
    return this.runSchedule(
      schedulerId,
      {
        every: everyMs,
      },
      input,
      opts,
    )
  }

  async exportPrometheusMetrics(globalVariables?: Record<string, string>) {
    const queue = await this.getOrCreateQueue()
    return queue.exportPrometheusMetrics({
      workflowId: this.id,
      workflowPrefix: Settings.defaultPrefix,
      ...globalVariables,
    })
  }

  private async getOrCreateQueue() {
    if (!this.queue) {
      this.queue = new Queue(this.opts.id, {
        prefix: Settings.defaultPrefix,
        connection: this.opts.connection ?? (await defaultRedisConnection()),
        defaultJobOptions: {
          removeOnComplete: true,
          removeOnFail: {
            age: 24 * 60 * 60, // 1 day
          },
          ...this.opts.queueOptions?.defaultJobOptions,
        },
        ...this.opts.queueOptions,
      })

      if (this.opts.queueOptions?.globalConcurrency)
        await this.queue.setGlobalConcurrency(this.opts.queueOptions.globalConcurrency)
      else await this.queue.removeGlobalConcurrency()

      if (this.opts.queueOptions?.globalRateLimit)
        await this.queue.setGlobalRateLimit(
          this.opts.queueOptions.globalRateLimit.max,
          this.opts.queueOptions.globalRateLimit.duration,
        )
      else await this.queue.removeGlobalRateLimit()
    }

    await this.queue.waitUntilReady()
    return this.queue
  }

  private async getOrCreateQueueEvents() {
    if (!this.queueEvents) {
      this.queueEvents = new QueueEvents(this.opts.id, {
        prefix: Settings.defaultPrefix,
        connection: this.opts.connection ?? (await defaultRedisConnection()),
        ...this.opts.queueEventsOptions,
      })
    }

    await this.queueEvents.waitUntilReady()
    return this.queueEvents
  }
}

export interface WorkflowRunContext<Input> {
  input: IsUnknown<Input> extends true ? undefined : Input
  step: WorkflowStep
  span: Span
}
