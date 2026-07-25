import type { Meter, Span } from '@opentelemetry/api'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type Redis from 'ioredis'
import type { IsUnknown } from 'type-fest'
import type { AddOptions, QueueOptions, ReservedJob, Worker, WorkerOptions } from './queue'
import type { WorkflowJobPayloadInternal, WorkflowQueueInternal } from './types'
import { context, propagation, ROOT_CONTEXT, SpanKind } from '@opentelemetry/api'
import { asyncExitHook } from 'exit-hook'
import { WorkflowJob } from './job'
import { Namespace } from './queue'
import { deserialize, serialize } from './serializer'
import { defaultRedisConnection, Settings } from './settings'
import { WorkflowStep } from './step'
import { runWithTracing } from './tracer'

/** Per-workflow queue overrides (module `QueueOptions` minus the id). */
export type WorkflowQueueOptions = Omit<QueueOptions, 'id'>

/** Per-workflow worker overrides plus the lib's OTel metrics binding. */
export type WorkflowWorkerOptions = WorkerOptions & {
  metrics?: {
    meter: Meter
    prefix: string
  }
}

/** Per-run enqueue overrides (`runAt`/`runIn` are set by `runAt()`/`runIn()`). */
export type WorkflowJobRunOptions = Omit<AddOptions, 'runAt' | 'runIn'>

export interface WorkflowNamespaceOptions {
  id: string
  concurrency?: number
  redis?: Redis
  prefix?: string
  queueOptions?: WorkflowQueueOptions
  workerOptions?: WorkflowWorkerOptions
  jobOptions?: WorkflowJobRunOptions
}

export interface CreateWorkflowOptions<RunInput, Input, Output> {
  id: string
  schema?: StandardSchemaV1<RunInput, Input>
  run: (ctx: WorkflowRunContext<Input>) => Promise<Output>
  getGroupId?: (
    input: IsUnknown<Input> extends true ? undefined : Input,
  ) => string | undefined | Promise<string | undefined>
  queueOptions?: WorkflowQueueOptions
  workerOptions?: WorkflowWorkerOptions
  jobOptions?: WorkflowJobRunOptions
}

export interface WorkflowScheduleOptions<RunInput> {
  pattern: string
  input: RunInput
  tz?: string
  priority?: number
  groupId?: string
  skipIfRunning?: boolean
}

/**
 * Owns the shared redis + single pub/sub connection (via the queue module's `Namespace`) and the
 * cross-workflow concurrency cap. Mints workflows whose namespace-level option bags are shallow-
 * merged under each workflow's own overrides. Default concurrency is unlimited (no cross-workflow
 * cap, preserving pre-namespace behavior). The module `Namespace` is created lazily so the default
 * (async) redis connection can be resolved on first use.
 */
export class WorkflowNamespace {
  readonly id: string
  private readonly opts: WorkflowNamespaceOptions
  private namespace?: Promise<Namespace>

  constructor(opts: WorkflowNamespaceOptions) {
    this.id = opts.id
    this.opts = opts
  }

  async getNamespace(): Promise<Namespace> {
    if (!this.namespace) {
      this.namespace = (async () => {
        const redis = this.opts.redis ?? (await defaultRedisConnection())
        const namespace = new Namespace({
          id: this.opts.id,
          concurrency: this.opts.concurrency,
          redis,
          prefix: this.opts.prefix,
        })
        // One namespace-level exit hook for the redis disconnect (cascades to queues/workers).
        asyncExitHook(async () => namespace.close(), { wait: 10_000 })
        return namespace
      })()
    }
    return this.namespace
  }

  createWorkflow<RunInput, Input = RunInput, Output = unknown>(
    opts: CreateWorkflowOptions<RunInput, Input, Output>,
  ): Workflow<RunInput, Input, Output> {
    return new Workflow(this, {
      ...opts,
      queueOptions: { ...this.opts.queueOptions, ...opts.queueOptions },
      workerOptions: { ...this.opts.workerOptions, ...opts.workerOptions },
      jobOptions: { ...this.opts.jobOptions, ...opts.jobOptions },
    })
  }

  /** Top-level cascade: closes every queue/worker and disconnects the shared connections. */
  async close(): Promise<void> {
    if (!this.namespace) return
    const namespace = await this.namespace
    await namespace.close()
  }
}

export class Workflow<RunInput, Input, Output> {
  readonly id: string
  private readonly ns: WorkflowNamespace
  private readonly opts: CreateWorkflowOptions<RunInput, Input, Output>
  private queue?: Promise<WorkflowQueueInternal>

  constructor(ns: WorkflowNamespace, opts: CreateWorkflowOptions<RunInput, Input, Output>) {
    this.ns = ns
    this.opts = opts
    this.id = opts.id
  }

  private async getQueue(): Promise<WorkflowQueueInternal> {
    if (!this.queue) {
      this.queue = (async () => {
        const namespace = await this.ns.getNamespace()
        return namespace.queue({
          id: this.opts.id,
          concurrency: this.opts.queueOptions?.concurrency,
          groupConcurrency: this.opts.queueOptions?.groupConcurrency,
          resultTtl: this.opts.queueOptions?.resultTtl,
        })
      })()
    }
    return this.queue
  }

  async work(opts?: WorkflowWorkerOptions): Promise<Worker> {
    const queue = await this.getQueue()
    const { metrics, ...workerOpts } = { ...this.opts.workerOptions, ...opts }

    const worker = queue.worker(
      async (job: ReservedJob, ctx) => {
        Settings.logger?.info?.(`[${this.id}] Processing job ${job.id}`)

        const deserializedData = deserialize<WorkflowJobPayloadInternal>(job.data)
        const parsedData =
          this.opts.schema && (await this.opts.schema['~standard'].validate(deserializedData.input))
        if (parsedData?.issues) throw new Error(`Invalid workflow input`)

        return runWithTracing(
          `workflow-worker/${this.id}`,
          {
            attributes: {
              'workflow.id': this.id,
              'workflow.job_id': job.id,
            },
            kind: SpanKind.CONSUMER,
          },
          async (span) => {
            const stepPromises = new Set<Promise<any>>()
            const start = performance.now()
            try {
              const result = await this.opts.run({
                // eslint-disable-next-line ts/no-unsafe-assignment
                input: parsedData?.value as any,
                step: new WorkflowStep({
                  queue,
                  workflowJobId: job.id,
                  workflowId: this.id,
                  signal: ctx.signal,
                  stepPromises,
                }),
                span,
              })

              Settings.logger?.success?.(
                `[${this.id}] Completed job ${job.id} in ${(performance.now() - start).toFixed(2)} ms`,
              )
              return serialize(result)
            } catch (err) {
              if (stepPromises.size > 0) {
                Settings.logger?.warn?.(
                  `[${this.id}] Job failed but there are still ${stepPromises.size} running step(s), waiting for them to finish. Be careful when using 'Promise.all([step0, step1, ...])', as running steps are not canceled when one of them fails.`,
                )
                await Promise.allSettled(stepPromises)
              }
              throw err
            }
          },
          propagation.extract(
            ROOT_CONTEXT,
            deserializedData.tracingHeaders as Record<string, string>,
          ),
        )
      },
      {
        ...workerOpts,
        onFailed:
          workerOpts.onFailed ??
          ((job, error) => Settings.logger?.error?.(`[${this.id}] Job ${job.id} failed:`, error)),
        onError:
          workerOpts.onError ??
          ((error) => Settings.logger?.error?.(`[${this.id}] Job error:`, error)),
      },
    )

    Settings.logger?.info?.(`[${this.id}] Worker started`)

    const metricsOpts = metrics ?? Settings.metrics
    if (metricsOpts) this.setupMetrics(queue, metricsOpts)

    asyncExitHook(
      async (signal) => {
        Settings.logger?.info?.(`[${this.id}] Received ${signal}, shutting down worker...`)
        await worker.close()
      },
      { wait: 10_000 },
    )

    return worker
  }

  async run(input: RunInput, opts?: WorkflowJobRunOptions): Promise<WorkflowJob<Output>> {
    return this.enqueue(input, opts)
  }

  async runIn(input: RunInput, delayMs: number, opts?: WorkflowJobRunOptions) {
    return this.enqueue(input, { ...opts, runIn: delayMs })
  }

  async runAt(input: RunInput, date: Date, opts?: WorkflowJobRunOptions) {
    return this.enqueue(input, { ...opts, runAt: date.getTime() })
  }

  private async enqueue(
    input: RunInput,
    opts?: WorkflowJobRunOptions & { runAt?: number; runIn?: number },
  ): Promise<WorkflowJob<Output>> {
    const parsedInput = this.opts.schema && (await this.opts.schema['~standard'].validate(input))
    if (parsedInput?.issues) throw new Error('Invalid workflow input')

    const queue = await this.getQueue()

    const groupId =
      opts?.groupId ??
      (await this.opts.getGroupId?.(
        parsedInput?.value as IsUnknown<Input> extends true ? undefined : Input,
      )) ??
      this.opts.jobOptions?.groupId

    return runWithTracing(
      `workflow-producer/${this.id}`,
      {
        attributes: { 'workflow.id': this.id },
        kind: SpanKind.PRODUCER,
      },
      async () => {
        const tracingHeaders = {}
        propagation.inject(context.active(), tracingHeaders)

        const job = await queue.add(
          serialize({ input: parsedInput?.value ?? input, tracingHeaders }),
          {
            groupId,
            priority: opts?.priority ?? this.opts.jobOptions?.priority,
            // Thread the worker/job `maxAttempts` default into `add` — the module applies
            // `maxAttempts` per-job at enqueue time, so a per-workflow retry default only takes
            // effect if the lib passes it here.
            maxAttempts:
              opts?.maxAttempts ??
              this.opts.jobOptions?.maxAttempts ??
              this.opts.workerOptions?.maxAttempts,
            jobId: opts?.jobId ?? this.opts.jobOptions?.jobId,
            runAt: opts?.runAt,
            runIn: opts?.runIn,
          },
        )

        return new WorkflowJob<Output>({ queue, jobId: job.id, groupId: job.groupId })
      },
    )
  }

  /**
   * Register (or idempotently replace) a cron schedule. The typed `input` is validated against
   * `schema` at registration time (fail fast) and stored pre-serialized as opaque `data`.
   */
  async upsertSchedule(scheduleId: string, opts: WorkflowScheduleOptions<RunInput>): Promise<void> {
    const parsed = this.opts.schema && (await this.opts.schema['~standard'].validate(opts.input))
    if (parsed?.issues) throw new Error('Invalid workflow input')

    const queue = await this.getQueue()
    await queue.upsertSchedule(scheduleId, {
      pattern: opts.pattern,
      data: serialize({ input: parsed?.value ?? opts.input, tracingHeaders: {} }),
      tz: opts.tz,
      priority: opts.priority,
      groupId: opts.groupId,
      skipIfRunning: opts.skipIfRunning,
    })
  }

  async removeSchedule(scheduleId: string): Promise<void> {
    const queue = await this.getQueue()
    await queue.removeSchedule(scheduleId)
  }

  async getSchedules() {
    const queue = await this.getQueue()
    return queue.getSchedules()
  }

  private setupMetrics(
    queue: WorkflowQueueInternal,
    { meter, prefix }: { meter: Meter; prefix: string },
  ) {
    const attributes = { workflow_id: this.id }

    const activeJobsGauge = meter.createObservableGauge(`${prefix}_workflow_active_jobs`, {
      description: 'Number of active workflow jobs',
    })
    const waitingJobsGauge = meter.createObservableGauge(`${prefix}_workflow_waiting_jobs`, {
      description: 'Number of waiting workflow jobs',
    })
    const delayedJobsGauge = meter.createObservableGauge(`${prefix}_workflow_delayed_jobs`, {
      description: 'Number of delayed workflow jobs',
    })

    meter.addBatchObservableCallback(
      async (observableResult) => {
        try {
          const { active, waiting, delayed } = await queue.getMetrics()
          observableResult.observe(activeJobsGauge, active, attributes)
          observableResult.observe(waitingJobsGauge, waiting, attributes)
          observableResult.observe(delayedJobsGauge, delayed, attributes)
        } catch (err) {
          Settings.logger?.error?.('Error collecting workflow metrics:', err)
        }
      },
      [activeJobsGauge, waitingJobsGauge, delayedJobsGauge],
    )
  }
}

export interface WorkflowRunContext<Input> {
  input: IsUnknown<Input> extends true ? undefined : Input
  step: WorkflowStep
  span: Span
}
