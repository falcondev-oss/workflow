import type { Span } from '@opentelemetry/api'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type {
  ConnectionOptions,
  JobsOptions,
  QueueEventsOptions,
  QueueOptions,
  WorkerOptions,
} from 'bullmq'
import type { Except, IsUnknown, SetOptional } from 'type-fest'
import type { Serialized } from './serializer'
import type { WorkflowJobInternal, WorkflowQueueInternal } from './types'
import { context, propagation, ROOT_CONTEXT, SpanKind } from '@opentelemetry/api'
import { Queue, QueueEvents, Worker } from 'bullmq'
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
  queueOptions?: SetOptional<QueueOptions, 'connection'>
  queueEventsOptions?: SetOptional<QueueEventsOptions, 'connection'>
  connection?: ConnectionOptions
}

export class Workflow<RunInput, Input, Output> {
  private opts
  private queue?: WorkflowQueueInternal<Input, Output>
  private queueEvents?: QueueEvents

  constructor(opts: WorkflowOptions<RunInput, Input, Output>) {
    this.opts = opts
  }

  async work(opts?: Omit<SetOptional<WorkerOptions, 'connection'>, 'autorun'>) {
    const queue = await this.getOrCreateQueue()

    const worker = new Worker<WorkflowJobInternal<Input, Output>['data'], Serialized<Output>>(
      this.opts.id,
      async (job) => {
        const jobId = job.id
        if (!jobId) throw new Error('Job ID is missing')

        const deserializedData = deserialize(job.data)
        const parsedData =
          this.opts.schema && (await this.opts.schema['~standard'].validate(deserializedData.input))
        if (parsedData?.issues)
          throw new WorkflowInputError('Invalid workflow input', parsedData.issues)

        return runWithTracing(
          `workflow:work:${this.opts.id}`,
          {
            attributes: {
              'workflow.id': this.opts.id,
              'workflow.job_id': jobId,
            },
            kind: SpanKind.CONSUMER,
          },
          async (span) => {
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
            return serialize(result)
          },
          propagation.extract(ROOT_CONTEXT, deserializedData.tracingHeaders),
        )
      },
      {
        connection: this.opts.connection ?? (await defaultRedisConnection()),
        prefix: Settings.defaultPrefix,
        ...opts,
      },
    )
    await worker.waitUntilReady()

    return this
  }

  async run(input: RunInput, opts?: JobsOptions) {
    const parsedInput = this.opts.schema && (await this.opts.schema['~standard'].validate(input))
    if (parsedInput?.issues)
      throw new WorkflowInputError('Invalid workflow input', parsedInput.issues)

    const queue = await this.getOrCreateQueue()
    return runWithTracing(
      `workflow:run:${this.opts.id}`,
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
            // eslint-disable-next-line ts/no-unsafe-assignment
            input: parsedInput?.value as any,
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

  async repeat(
    input: RunInput,
    cronOrInterval: string | number,
    opts?: Except<JobsOptions, 'repeat'>,
  ) {
    return this.run(input, {
      repeat: {
        tz: Settings.defaultCronTimezone,
        ...(typeof cronOrInterval === 'string'
          ? {
              pattern: cronOrInterval,
            }
          : {
              every: cronOrInterval,
            }),
      },
      ...opts,
    })
  }

  private async getOrCreateQueue() {
    if (!this.queue) {
      this.queue = new Queue(this.opts.id, {
        prefix: Settings.defaultPrefix,
        connection: this.opts.connection ?? (await defaultRedisConnection()),
        ...this.opts.queueOptions,
      })
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
