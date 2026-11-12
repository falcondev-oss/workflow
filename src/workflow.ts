import type { Meter, Span } from '@opentelemetry/api'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type Redis from 'ioredis'
import type { IsUnion, IsUnknown } from 'type-fest'
import type {
  AddOptions,
  MetricsOptions,
  QueueOptions,
  ReservedJob,
  Worker,
  WorkerOptions,
  WorkflowLogger,
} from './queue'
import type { RateLimiterBudget, RateLimiterMetrics } from './queue/types'
import type { WorkflowJobPayloadInternal, WorkflowQueueInternal } from './types'
import { randomUUID } from 'node:crypto'
import { context, propagation, ROOT_CONTEXT, SpanKind } from '@opentelemetry/api'
import { asyncExitHook } from 'exit-hook'
import { WorkflowJob } from './job'
import { Namespace, NonRecoverableError } from './queue'
import { RateLimitError } from './queue/errors'
import { createWorkerMetrics } from './queue/metrics'
import { deserialize, serialize } from './serializer'
import { defaultRedisConnection } from './settings'
import { WorkflowStep } from './step'
import { runWithTracing } from './tracer'

export type { WorkflowEvent } from './job'

export type WorkflowQueueOptions<Names extends readonly string[] = readonly string[]> = Omit<
  QueueOptions,
  'id' | 'rateLimiters'
> & {
  rateLimiters?: Names
}

export type WorkflowWorkerOptions = WorkerOptions & {
  metrics?: MetricsOptions
}

export type WorkflowJobRunOptions = Omit<AddOptions, 'runAt' | 'runIn'>

export interface WorkflowNamespaceOptions<
  Limiters extends Record<string, RateLimiterBudget> = Record<string, RateLimiterBudget>,
  Names extends readonly (keyof Limiters & string)[] = readonly (keyof Limiters & string)[],
> {
  id: string
  concurrency?: number
  redis?: Redis
  prefix?: string
  logger?: WorkflowLogger
  autoClose?: boolean
  rateLimiters?: Limiters
  queueOptions?: WorkflowQueueOptions<Names>
  workerOptions?: WorkflowWorkerOptions
  jobOptions?: WorkflowJobRunOptions
}

export type WorkflowHandler<
  Input,
  Output,
  ProgressInput = never,
  Names extends readonly string[] = readonly string[],
> = (ctx: WorkflowRunContext<Input, ProgressInput, Names[number]>) => Promise<Output>

export interface CreateWorkflowOptions<
  RunInput,
  Input,
  Output,
  ProgressInput = never,
  Progress = ProgressInput,
  Names extends readonly string[] = readonly string[],
> extends DefineWorkflowOptions<RunInput, Input, ProgressInput, Progress, Names> {
  run: WorkflowHandler<Input, Output, ProgressInput, Names>
}

export interface DefineWorkflowOptions<
  RunInput,
  Input,
  ProgressInput = never,
  Progress = ProgressInput,
  Names extends readonly string[] = readonly string[],
> {
  id: string
  schema?: StandardSchemaV1<RunInput, Input>
  progressSchema?: StandardSchemaV1<ProgressInput, Progress>
  getGroupId?: (
    input: IsUnknown<Input> extends true ? undefined : Input,
  ) => string | undefined | Promise<string | undefined>
  queueOptions?: WorkflowQueueOptions<Names>
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

export class WorkflowNamespace<
  const Limiters extends Record<string, RateLimiterBudget> = Record<never, never>,
  const Defaults extends readonly (keyof Limiters & string)[] = [],
> {
  private readonly opts: WorkflowNamespaceOptions<Limiters, Defaults>
  private namespace?: Promise<Namespace>
  private unregisterExitHook?: () => void
  readonly id: string
  readonly logger?: WorkflowLogger

  constructor(opts: WorkflowNamespaceOptions<Limiters, Defaults>) {
    const rateLimiters = Object.entries(opts.rateLimiters ?? {})
    for (const [name, { limit, window }] of rateLimiters) {
      if (
        !Number.isSafeInteger(limit) ||
        limit <= 0 ||
        !Number.isSafeInteger(window) ||
        window <= 0
      )
        throw new Error(`Rate limiter "${name}" requires positive integer limit and window`)
    }
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
          rateLimiters: this.opts.rateLimiters,
        })
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
    const Names extends readonly (keyof Limiters & string)[] = Defaults,
  >(
    opts: CreateWorkflowOptions<RunInput, Input, Output, ProgressInput, Progress, Names>,
  ): Workflow<RunInput, Input, Output, ProgressInput, Progress, Names> {
    return this.defineWorkflow(opts).returns<Output>().handle(opts.run)
  }

  defineWorkflow<
    RunInput,
    Input = RunInput,
    ProgressInput = never,
    Progress = ProgressInput,
    const Names extends readonly (keyof Limiters & string)[] = Defaults,
  >(
    opts: DefineWorkflowOptions<RunInput, Input, ProgressInput, Progress, Names>,
  ): WorkflowDefinition<RunInput, Input, unknown, ProgressInput, Progress, Names> {
    return new WorkflowDefinition(this, {
      ...opts,
      queueOptions: {
        ...this.opts.queueOptions,
        ...opts.queueOptions,
        rateLimiters: (opts.queueOptions?.rateLimiters ?? this.opts.queueOptions?.rateLimiters) as
          Names | undefined,
      },
      workerOptions: { ...this.opts.workerOptions, ...opts.workerOptions },
      jobOptions: { ...this.opts.jobOptions, ...opts.jobOptions },
    })
  }

  async getRateLimiterMetrics(): Promise<Record<keyof Limiters & string, RateLimiterMetrics>> {
    const namespace = await this.getNamespace()
    return namespace.getRateLimiterMetrics()
  }

  async close(): Promise<void> {
    if (!this.namespace) return
    this.unregisterExitHook?.()
    this.unregisterExitHook = undefined
    const namespace = await this.namespace
    await namespace.close()
  }
}

export class WorkflowDefinition<
  RunInput,
  Input,
  Output,
  ProgressInput = never,
  Progress = ProgressInput,
  Names extends readonly string[] = readonly string[],
> {
  private readonly ns: Pick<WorkflowNamespace, 'getNamespace' | 'logger'>
  private queue?: Promise<WorkflowQueueInternal>
  readonly id: string
  protected readonly opts: DefineWorkflowOptions<RunInput, Input, ProgressInput, Progress, Names>

  constructor(
    ns: Pick<WorkflowNamespace, 'getNamespace' | 'logger'>,
    opts: DefineWorkflowOptions<RunInput, Input, ProgressInput, Progress, Names>,
  ) {
    this.ns = ns
    this.opts = opts
    this.id = opts.id
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
            maxAttempts:
              opts?.maxAttempts ??
              this.opts.jobOptions?.maxAttempts ??
              this.opts.workerOptions?.maxAttempts,
            jobId: opts?.jobId ?? this.opts.jobOptions?.jobId,
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

  protected get logger() {
    return this.ns.logger
  }

  protected async getQueue(): Promise<WorkflowQueueInternal> {
    if (!this.queue) {
      this.queue = (async () => {
        const namespace = await this.ns.getNamespace()
        return namespace.queue({
          id: this.opts.id,
          concurrency: this.opts.queueOptions?.concurrency,
          groupConcurrency: this.opts.queueOptions?.groupConcurrency,
          resultTtl: this.opts.queueOptions?.resultTtl,
          rateLimiters: this.opts.queueOptions?.rateLimiters,
        })
      })()
    }
    return this.queue
  }

  returns<T>(this: this & { work?: never }) {
    return new WorkflowDefinition<RunInput, Input, T, ProgressInput, Progress, Names>(
      this.ns,
      this.opts,
    )
  }

  handle<HandlerOutput extends Output>(
    this: this & { work?: never },
    run: WorkflowHandler<Input, HandlerOutput, ProgressInput, Names>,
  ): Workflow<
    RunInput,
    Input,
    unknown extends Output ? HandlerOutput : Output,
    ProgressInput,
    Progress,
    Names
  > {
    return new Workflow(this.ns, { ...this.opts, run })
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

  async getGroupJobs(groupId: string) {
    const queue = await this.getQueue()
    const jobs = await queue.getGroupJobs(groupId)
    return jobs.map(({ data, ...job }) => ({
      ...job,
      input: deserialize<WorkflowJobPayloadInternal>(data).input as Input,
    }))
  }

  async getMetrics() {
    const queue = await this.getQueue()
    return queue.getMetrics()
  }
}

export class Workflow<
  RunInput,
  Input,
  Output,
  ProgressInput = never,
  Progress = ProgressInput,
  Names extends readonly string[] = readonly string[],
> extends WorkflowDefinition<RunInput, Input, Output, ProgressInput, Progress, Names> {
  declare protected readonly opts: CreateWorkflowOptions<
    RunInput,
    Input,
    Output,
    ProgressInput,
    Progress,
    Names
  >

  constructor(
    ns: Pick<WorkflowNamespace, 'getNamespace' | 'logger'>,
    opts: CreateWorkflowOptions<RunInput, Input, Output, ProgressInput, Progress, Names>,
  ) {
    super(ns, opts)
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
    const failedJobsGauge = meter.createObservableGauge(`${prefix}_workflow_failed_jobs`, {
      description: 'Number of retained dead-lettered workflow jobs',
    })

    meter.addBatchObservableCallback(
      async (observableResult) => {
        try {
          const { active, waiting, delayed, failed } = await queue.getMetrics()
          observableResult.observe(activeJobsGauge, active, attributes)
          observableResult.observe(waitingJobsGauge, waiting, attributes)
          observableResult.observe(delayedJobsGauge, delayed, attributes)
          observableResult.observe(failedJobsGauge, failed, attributes)
        } catch (err) {
          this.logger?.error?.('Error collecting workflow metrics:', err)
        }
      },
      [activeJobsGauge, waitingJobsGauge, delayedJobsGauge, failedJobsGauge],
    )
  }

  async work(opts?: WorkflowWorkerOptions): Promise<Worker> {
    const queue = await this.getQueue()
    const { metrics, ...workerOpts } = { ...this.opts.workerOptions, ...opts }
    const caps = [this.opts.workerOptions?.concurrency, opts?.concurrency].filter(
      (cap) => cap !== undefined,
    )
    if (caps.length > 0) workerOpts.concurrency = Math.min(...caps)
    const workerMetrics = metrics && createWorkerMetrics(metrics, this.id)

    const worker = queue.worker(
      async (job: ReservedJob, ctx) => {
        this.logger?.info?.(`[${this.id}] Processing job ${job.id}`)

        const deserializedData = deserialize<WorkflowJobPayloadInternal>(job.data)
        const parsedData =
          this.opts.schema && (await this.opts.schema['~standard'].validate(deserializedData.input))
        if (parsedData?.issues) {
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
              'workflow.queue_wait_ms': job.queueWaitMs,
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
                  recordStepDuration: workerMetrics?.recordStep,
                }),
                span,
                signal: ctx.signal,
                rateLimit: (ms: number, name?: string): never => {
                  if (!Number.isFinite(ms) || ms <= 0)
                    throw new Error('rateLimit ms must be a positive finite number')
                  const limiter = name ?? this.opts.queueOptions?.rateLimiters?.[0] ?? ''
                  span.addEvent('workflow.rate_limit', {
                    ...(limiter && { 'workflow.rate_limiter': limiter }),
                    'workflow.rate_limit.pause_ms': ms,
                  })
                  throw new RateLimitError(ms, limiter)
                },
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
      workerMetrics,
    )

    this.logger?.info?.(`[${this.id}] Worker started`)

    if (metrics) {
      this.setupMetrics(queue, metrics)
      queue.ns.setupRateLimiterMetrics(metrics)
    }

    return worker
  }
}

type RateLimit<Names extends string> = [Names] extends [never]
  ? (ms: number) => never
  : IsUnion<Names> extends true
    ? (ms: number, limiter: Names) => never
    : (ms: number, limiter?: Names) => never

export interface WorkflowRunContext<Input, Progress = never, Names extends string = never> {
  input: IsUnknown<Input> extends true ? undefined : Input
  step: WorkflowStep<Progress>
  span: Span
  signal: AbortSignal
  rateLimit: RateLimit<Names>
}
