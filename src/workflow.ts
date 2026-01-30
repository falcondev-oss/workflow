import type { Meter, Span } from '@opentelemetry/api'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type { AddOptions, QueueOptions, WorkerOptions } from 'groupmq'
import type Redis from 'ioredis'
import type { Except, IsUnknown, SetOptional } from 'type-fest'
import type { WorkflowJobPayloadInternal, WorkflowQueueInternal } from './types'
import { randomUUID } from 'node:crypto'
import { context, propagation, ROOT_CONTEXT, SpanKind } from '@opentelemetry/api'
import { asyncExitHook } from 'exit-hook'
import { Queue, Worker } from 'groupmq'
import { WorkflowJob } from './job'
import { deserialize, serialize } from './serializer'
import { defaultRedisConnection, Settings } from './settings'
import { WorkflowStep } from './step'
import { runWithTracing } from './tracer'

export interface WorkflowOptions<RunInput, Input, Output> {
  id: string
  schema?: StandardSchemaV1<RunInput, Input>
  run: (ctx: WorkflowRunContext<Input>) => Promise<Output>
  getGroupId?: (
    input: IsUnknown<Input> extends true ? undefined : Input,
  ) => string | undefined | Promise<string | undefined>
  queueOptions?: WorkflowQueueOptions
  workerOptions?: WorkflowWorkerOptions<Input>
  jobOptions?: WorkflowJobRunOptions<Input>
  redis?: Redis
}

export type WorkflowJobRunOptions<Input> = SetOptional<
  Except<AddOptions<WorkflowJobPayloadInternal<Input>>, 'data'>,
  'groupId'
> & {
  priority?: 'high' | 'normal'
}

export type WorkflowQueueOptions = SetOptional<Except<QueueOptions, 'namespace'>, 'redis'>

export type WorkflowWorkerOptions<Input> = Except<
  WorkerOptions<WorkflowJobPayloadInternal<Input>>,
  'queue' | 'handler' | 'name'
> & {
  metrics?: {
    meter: Meter
    prefix: string
  }
}

export class Workflow<RunInput, Input, Output> {
  'id'
  private 'opts'
  private 'queue'?: WorkflowQueueInternal<Input>
  '~internal' = {
    getOrCreateQueue: this.getOrCreateQueue.bind(this),
  }

  'constructor'(opts: WorkflowOptions<RunInput, Input, Output>) {
    this.id = opts.id
    this.opts = opts
  }

  async 'work'(opts?: WorkflowWorkerOptions<Input>) {
    const queue = await this.getOrCreateQueue()

    const worker: Worker<WorkflowJobPayloadInternal<Input>> = new Worker({
      handler: async (job) => {
        Settings.logger?.info?.(`[${this.opts.id}] Processing job ${job.id} `)
        const jobId = job.id
        if (!jobId) throw new Error('Job ID is missing')

        const deserializedData = deserialize(job.data)
        const parsedData =
          this.opts.schema && (await this.opts.schema['~standard'].validate(deserializedData.input))
        if (parsedData?.issues) throw new Error(`Invalid workflow input`)

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
              `[${this.opts.id}] Completed job ${job.id} in ${(end - start).toFixed(2)} ms`,
            )
            return serialize(result)
          },
          propagation.extract(ROOT_CONTEXT, deserializedData.tracingHeaders),
        )
      },
      queue,
      ...this.opts.workerOptions,
      ...opts,
    })

    worker.on('failed', (job) => {
      Settings.logger?.info?.(`[${this.opts.id}] Job ${job.id} failed`)
    })
    worker.on('error', (error) => {
      Settings.logger?.error?.(`[${this.opts.id}] Error during worker startup:`, error)
    })

    Settings.logger?.info?.(`[${this.opts.id}] Worker started`)

    const metricsOpts = opts?.metrics ?? this.opts.workerOptions?.metrics ?? Settings.metrics
    if (metricsOpts) await this.setupMetrics(metricsOpts)

    asyncExitHook(
      async (signal) => {
        Settings.logger?.info?.(`[${this.opts.id}] Received ${signal}, shutting down worker...`)
        await worker.close()
      },
      { wait: 10_000 },
    )

    return worker
  }

  async 'run'(input: RunInput, opts?: WorkflowJobRunOptions<Input>) {
    const parsedInput = this.opts.schema && (await this.opts.schema['~standard'].validate(input))
    if (parsedInput?.issues) throw new Error('Invalid workflow input')

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

        const orderMs =
          opts?.orderMs ??
          (opts?.priority === 'high' ? 0 : undefined) ??
          this.opts.jobOptions?.orderMs ??
          (this.opts.jobOptions?.priority === 'high' ? 0 : undefined)

        const job = await queue.add({
          groupId:
            (await this.opts.getGroupId?.(
              parsedInput?.value as IsUnknown<Input> extends true ? undefined : Input,
            )) ?? randomUUID(),
          data: serialize({
            input: parsedInput?.value,
            stepData: {},
            tracingHeaders,
          }),
          ...this.opts.jobOptions,
          ...opts,
          orderMs,
        })

        return new WorkflowJob<Output>({
          job,
        })
      },
    )
  }

  async 'runIn'(input: RunInput, delayMs: number, opts?: WorkflowJobRunOptions<Input>) {
    return this.run(input, {
      delay: delayMs,
      ...opts,
    })
  }

  async 'runAt'(input: RunInput, date: Date, opts?: WorkflowJobRunOptions<Input>) {
    return this.run(input, {
      runAt: date,
      ...opts,
    })
  }

  async 'runCron'(
    scheduleId: string,
    cron: string,
    input: RunInput,
    opts?: WorkflowJobRunOptions<Input>,
  ) {
    return this.run(input, {
      groupId: scheduleId,
      repeat: {
        pattern: cron,
      },
      ...opts,
    })
  }

  async 'runEvery'(
    scheduleId: string,
    everyMs: number,
    input: RunInput,
    opts?: WorkflowJobRunOptions<Input>,
  ) {
    return this.run(input, {
      groupId: scheduleId,
      repeat: {
        every: everyMs,
      },
      ...opts,
    })
  }

  private async 'getOrCreateQueue'() {
    if (!this.queue) {
      this.queue = new Queue({
        namespace: this.opts.id,
        redis: this.opts.redis ?? (await defaultRedisConnection()),
        keepFailed: 100,
        ...this.opts.queueOptions,
      })
    }

    return this.queue
  }

  private async 'setupMetrics'({ meter, prefix }: { meter: Meter; prefix: string }) {
    const attributes = {
      workflow_id: this.opts.id,
    }

    const queue = await this.getOrCreateQueue()

    const completedJobsGauge = meter.createObservableGauge(`${prefix}_workflow_completed_jobs`, {
      description: 'Number of completed workflow jobs',
    })
    const activeJobsGauge = meter.createObservableGauge(`${prefix}_workflow_active_jobs`, {
      description: 'Number of active workflow jobs',
    })
    const failedJobsGauge = meter.createObservableGauge(`${prefix}_workflow_failed_jobs`, {
      description: 'Number of failed workflow jobs',
    })
    const waitingJobsGauge = meter.createObservableGauge(`${prefix}_workflow_waiting_jobs`, {
      description: 'Number of waiting workflow jobs',
    })
    const delayedJobsGauge = meter.createObservableGauge(`${prefix}_workflow_delayed_jobs`, {
      description: 'Number of delayed workflow jobs',
    })
    const groupCountGauge = meter.createObservableGauge(`${prefix}_workflow_groups`, {
      description: 'Number of workflow job groups',
    })

    meter.addBatchObservableCallback(
      async (observableResult) => {
        try {
          const [counts, groupCount] = await Promise.all([
            queue.getJobCounts(),
            queue.getUniqueGroupsCount(),
          ])

          observableResult.observe(completedJobsGauge, counts.completed, attributes)
          observableResult.observe(activeJobsGauge, counts.active, attributes)
          observableResult.observe(failedJobsGauge, counts.failed, attributes)
          observableResult.observe(waitingJobsGauge, counts.waiting, attributes)
          observableResult.observe(delayedJobsGauge, counts.delayed, attributes)
          observableResult.observe(groupCountGauge, groupCount, attributes)
        } catch (err) {
          console.error('Error collecting workflow metrics:', err)
        }
      },
      [
        completedJobsGauge,
        activeJobsGauge,
        failedJobsGauge,
        waitingJobsGauge,
        delayedJobsGauge,
        groupCountGauge,
      ],
    )
  }
}

export interface WorkflowRunContext<Input> {
  input: IsUnknown<Input> extends true ? undefined : Input
  step: WorkflowStep
  span: Span
}
