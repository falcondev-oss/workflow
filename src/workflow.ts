import type { Meter, Span } from '@opentelemetry/api'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type Redis from 'ioredis'
import type { IsUnknown } from 'type-fest'
import type {
  AddOptions,
  QueueOptions,
  ReservedJob,
  Worker,
  WorkerOptions,
  WorkflowLogger,
} from './queue'
import type { WorkflowJobPayloadInternal, WorkflowQueueInternal } from './types'
import { randomUUID } from 'node:crypto'
import { context, propagation, ROOT_CONTEXT, SpanKind } from '@opentelemetry/api'
import { asyncExitHook } from 'exit-hook'
import { WorkflowJob } from './job'
import { Namespace, NonRecoverableError } from './queue'
import { deserialize, serialize } from './serializer'
import { defaultRedisConnection } from './settings'
import { WorkflowStep } from './step'
import { runWithTracing } from './tracer'

export type { WorkflowEvent } from './job'

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
  /** Logger inherited by every workflow, queue and worker of this namespace. Default: none. */
  logger?: WorkflowLogger
  /**
   * Close the namespace (draining workers, disconnecting redis) on `SIGINT`/`SIGTERM`/exit.
   * Default: true.
   */
  autoClose?: boolean
  queueOptions?: WorkflowQueueOptions
  workerOptions?: WorkflowWorkerOptions
  jobOptions?: WorkflowJobRunOptions
}

export interface CreateWorkflowOptions<
  RunInput,
  Input,
  Output,
  ProgressInput = never,
  Progress = ProgressInput,
> {
  id: string
  schema?: StandardSchemaV1<RunInput, Input>
  progressSchema?: StandardSchemaV1<ProgressInput, Progress>
  run: (ctx: WorkflowRunContext<Input, ProgressInput>) => Promise<Output>
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
 * cap). The module `Namespace` is created lazily so the default (async) redis connection can be
 * resolved on first use.
 */
export class WorkflowNamespace {
  readonly id: string
  readonly logger?: WorkflowLogger
  private readonly opts: WorkflowNamespaceOptions
  private namespace?: Promise<Namespace>
  private unregisterExitHook?: () => void

  constructor(opts: WorkflowNamespaceOptions) {
    this.id = opts.id
    this.logger = opts.logger
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
          logger: this.opts.logger,
        })
        // One namespace-level exit hook: drains every worker and disconnects redis on a
        // process signal. Opt out with `autoClose: false` to own shutdown yourself.
        if (this.opts.autoClose ?? true) {
          this.unregisterExitHook = asyncExitHook(
            async (signal) => {
              this.logger?.info?.(`[${this.id}] Received ${signal}, closing namespace...`)
              await namespace.close()
            },
            { wait: 10_000 },
          )
        }
        return namespace
      })()
    }
    return this.namespace
  }

  createWorkflow<
    RunInput,
    Input = RunInput,
    Output = unknown,
    ProgressInput = never,
    Progress = ProgressInput,
  >(
    opts: CreateWorkflowOptions<RunInput, Input, Output, ProgressInput, Progress>,
  ): Workflow<RunInput, Input, Output, ProgressInput, Progress> {
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
    // Drop the exit hook first: an explicit close means a later signal must not close again.
    this.unregisterExitHook?.()
    this.unregisterExitHook = undefined
    const namespace = await this.namespace
    await namespace.close()
  }
}

export class Workflow<RunInput, Input, Output, ProgressInput = never, Progress = ProgressInput> {
  readonly id: string
  private readonly ns: WorkflowNamespace
  private readonly opts: CreateWorkflowOptions<RunInput, Input, Output, ProgressInput, Progress>
  private queue?: Promise<WorkflowQueueInternal>

  constructor(
    ns: WorkflowNamespace,
    opts: CreateWorkflowOptions<RunInput, Input, Output, ProgressInput, Progress>,
  ) {
    this.ns = ns
    this.opts = opts
    this.id = opts.id
  }

  private get logger() {
    return this.ns.logger
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
        this.logger?.info?.(`[${this.id}] Processing job ${job.id}`)

        const deserializedData = deserialize<WorkflowJobPayloadInternal>(job.data)
        const parsedData =
          this.opts.schema && (await this.opts.schema['~standard'].validate(deserializedData.input))
        if (parsedData?.issues) {
          // Stored payload no longer matches the schema — typically a job enqueued before a
          // schema change. Retrying re-reads the same payload, so this can never succeed.
          this.logger?.warn?.(
            `[${this.id}] Job ${job.id} data does not match the workflow schema (stale payload from an older schema version?):`,
            parsedData.issues,
          )
          throw new NonRecoverableError(`Invalid workflow input for job ${job.id}`, {
            cause: parsedData.issues,
          })
        }

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
                step: new WorkflowStep<ProgressInput>({
                  queue,
                  workflowJobId: job.id,
                  workflowId: this.id,
                  signal: ctx.signal,
                  stepPromises,
                  memo: new Map(job.steps),
                }),
                span,
              })

              this.logger?.success?.(
                `[${this.id}] Completed job ${job.id} in ${(performance.now() - start).toFixed(2)} ms`,
              )
              return serialize(result)
            } catch (err) {
              if (stepPromises.size > 0) {
                this.logger?.warn?.(
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
          ((job, error) => this.logger?.error?.(`[${this.id}] Job ${job.id} failed:`, error)),
        onError:
          workerOpts.onError ?? ((error) => this.logger?.error?.(`[${this.id}] Job error:`, error)),
      },
    )

    this.logger?.info?.(`[${this.id}] Worker started`)

    if (metrics) this.setupMetrics(queue, metrics)

    return worker
  }

  async run(input: RunInput, opts?: WorkflowJobRunOptions): Promise<WorkflowJob<Output, Progress>> {
    return this.enqueue(input, opts)
  }

  async runAndWatch(input: RunInput, opts?: WorkflowJobRunOptions) {
    const jobId = opts?.jobId ?? this.opts.jobOptions?.jobId ?? randomUUID()
    const events = await new WorkflowJob<Output, Progress>({
      queue: await this.getQueue(),
      jobId,
      progressSchema: this.opts.progressSchema,
      watchBeforeEnqueue: true,
    }).watch()
    try {
      const job = await this.enqueue(input, { ...opts, jobId })
      return { job, events }
    } catch (err) {
      await events.cancel(err)
      throw err
    }
  }

  async getJob(jobId: string): Promise<WorkflowJob<Output, Progress>> {
    return new WorkflowJob<Output, Progress>({
      queue: await this.getQueue(),
      jobId,
      progressSchema: this.opts.progressSchema,
    })
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
  ): Promise<WorkflowJob<Output, Progress>> {
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
            // `maxAttempts` is applied per-job at enqueue time, so a per-workflow retry default
            // only takes effect if it is passed here.
            maxAttempts:
              opts?.maxAttempts ??
              this.opts.jobOptions?.maxAttempts ??
              this.opts.workerOptions?.maxAttempts,
            jobId: opts?.jobId ?? this.opts.jobOptions?.jobId,
            // `runAt`/`runIn` are mutually exclusive; pass only the one that was set.
            ...(opts?.runIn === undefined ? { runAt: opts?.runAt } : { runIn: opts.runIn }),
          },
        )

        return new WorkflowJob<Output, Progress>({
          queue,
          jobId: job.id,
          progressSchema: this.opts.progressSchema,
        })
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

  /** Point-in-time queue-depth gauges (`active`/`waiting`/`delayed`) for this workflow. */
  async getMetrics() {
    const queue = await this.getQueue()
    return queue.getMetrics()
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
          this.logger?.error?.('Error collecting workflow metrics:', err)
        }
      },
      [activeJobsGauge, waitingJobsGauge, delayedJobsGauge],
    )
  }
}

export interface WorkflowRunContext<Input, Progress = never> {
  input: IsUnknown<Input> extends true ? undefined : Input
  step: WorkflowStep<Progress>
  span: Span
}
