import type { Span } from '@opentelemetry/api'
import type { WorkflowQueueInternal } from './types'
import { setTimeout } from 'node:timers/promises'
import { deserialize, serialize } from './serializer'
import { runWithTracing } from './tracer'

export type WorkflowStepData =
  | {
      type: 'do'
      result?: unknown
    }
  | {
      type: 'wait'
      durationMs: number
      startedAt: number
    }

export class WorkflowStep {
  private workflowId
  private queue
  private workflowJobId
  private stepNamePrefix
  private signal
  private stepPromises
  private logger

  constructor(opts: {
    queue: WorkflowQueueInternal
    workflowJobId: string
    workflowId: string
    stepNamePrefix?: string
    signal: AbortSignal
    stepPromises: Set<Promise<any>>
  }) {
    this.queue = opts.queue
    this.workflowJobId = opts.workflowJobId
    this.workflowId = opts.workflowId
    this.stepNamePrefix = opts.stepNamePrefix ? `${opts.stepNamePrefix}|` : ''
    this.signal = opts.signal
    this.stepPromises = opts.stepPromises
    this.logger = opts.queue.logger
  }

  private addNamePrefix(name: string) {
    return `${this.stepNamePrefix}${name}`
  }

  async do<R>(stepName: string, run: (ctx: { step: WorkflowStep; span: Span }) => R) {
    const name = this.addNamePrefix(stepName)

    // Memoize on completion only: a stored `result` field means the step finished on a prior
    // attempt, so a job-level retry replays it from cache and only re-runs the failed step.
    const stepData = await this.getStepData('do', name)
    if (stepData && 'result' in stepData) {
      this.logger?.debug?.(
        `[${this.workflowId}/${this.workflowJobId}] Step '${name}' already completed, returning cached result`,
      )
      return stepData.result as R
    }

    // Cooperative cancellation: bail before starting new work if the claim was lost.
    this.signal.throwIfAborted()

    this.logger?.debug?.(`[${this.workflowId}/${this.workflowJobId}] Running step '${name}'`)
    const promise = runWithTracing(
      `workflow-worker/${this.workflowId}/step/${name}`,
      {
        attributes: {
          'workflow.id': this.workflowId,
          'workflow.job_id': this.workflowJobId,
          'workflow.step_name': name,
        },
      },
      async (span) => {
        const result = await run({
          step: new WorkflowStep({
            queue: this.queue,
            workflowId: this.workflowId,
            workflowJobId: this.workflowJobId,
            stepNamePrefix: name,
            signal: this.signal,
            stepPromises: this.stepPromises,
          }),
          span,
        })

        await this.updateStepData(name, { type: 'do', result })

        this.logger?.debug?.(`[${this.workflowId}/${this.workflowJobId}] Completed step '${name}'`)

        return result
      },
    )

    this.stepPromises.add(promise)
    return promise.finally(() => this.stepPromises.delete(promise))
  }

  async wait(stepName: string, durationMs: number) {
    const name = this.addNamePrefix(stepName)

    const existingStepData = await this.getStepData('wait', name)

    const now = Date.now()
    const stepData = existingStepData ?? {
      type: 'wait' as const,
      durationMs,
      startedAt: now,
    }

    if (!existingStepData) await this.updateStepData(name, stepData)

    await runWithTracing(
      `workflow-worker/${this.workflowId}/step/${name}`,
      {
        attributes: {
          'workflow.id': this.workflowId,
          'workflow.job_id': this.workflowJobId,
          'workflow.step_name': name,
        },
      },
      async () => {
        this.logger?.debug?.(
          `[${this.workflowId}/${this.workflowJobId}] Waiting in step '${name}' for ${durationMs} ms`,
        )
        // Durable sleep survives resume: remaining time is computed from the persisted start.
        // Signal-aware so a lost claim aborts the sleep instead of holding slots until it elapses.
        const remainingMs = Math.max(0, stepData.startedAt + stepData.durationMs - Date.now())
        await setTimeout(remainingMs, undefined, { signal: this.signal })
      },
    )
  }

  async waitUntil(stepName: string, date: Date) {
    const now = Date.now()
    const targetTime = date.getTime()
    const durationMs = Math.max(0, targetTime - now)
    return this.wait(stepName, durationMs)
  }

  private async getStepData<T extends WorkflowStepData['type']>(type: T, stepName: string) {
    const raw = await this.queue.getStepData(this.workflowJobId, stepName)
    if (raw === null) return

    const stepData = deserialize<WorkflowStepData>(raw)
    if (stepData.type !== type)
      throw new Error(`Step "${stepName}" is of type "${stepData.type}", expected "${type}"`)

    return stepData as Extract<WorkflowStepData, { type: T }>
  }

  /** Per-field superjson persist — a single atomic `HSET` on the job's `:steps` hash. */
  private async updateStepData(stepName: string, data: WorkflowStepData) {
    await this.queue.setStepData(this.workflowJobId, stepName, serialize(data))
  }
}
