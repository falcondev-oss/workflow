import type { Span } from '@opentelemetry/api'
import type { Options } from 'p-retry'
import type { WorkflowQueueInternal } from './types'
import { setTimeout } from 'node:timers/promises'
import { UnrecoverableError } from 'bullmq'
import pRetry from 'p-retry'
import { deserialize, serialize } from './serializer'
import { runWithTracing } from './tracer'

export type WorkflowStepData =
  | {
      type: 'do'
      result?: unknown
      attempt: number
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

  constructor(opts: {
    queue: WorkflowQueueInternal<any, any>
    workflowJobId: string
    workflowId: string
    stepNamePrefix?: string
  }) {
    this.queue = opts.queue
    this.workflowJobId = opts.workflowJobId
    this.workflowId = opts.workflowId
    this.stepNamePrefix = opts.stepNamePrefix ? `${opts.stepNamePrefix}|` : ''
  }

  private addNamePrefix(name: string) {
    return `${this.stepNamePrefix}${name}`
  }

  async do<R>(
    stepName: string,
    run: (ctx: { step: WorkflowStep; span: Span }) => R,
    options?: WorkflowStepOptions,
  ) {
    const name = this.addNamePrefix(stepName)

    const stepData = await this.getStepData('do', name)
    if (stepData?.result) return stepData.result as R

    const initialAttempt = stepData?.attempt ?? 0
    await this.updateStepData(name, {
      type: 'do',
      attempt: initialAttempt,
    })
    return pRetry(
      async (attempt) => {
        const result = await runWithTracing(
          `workflow-worker/${this.workflowId}/step/${name}`,
          {
            attributes: {
              'workflow.id': this.workflowId,
              'workflow.job_id': this.workflowJobId,
              'workflow.step_name': name,
              'workflow.step.attempt': attempt,
            },
          },
          async (span) =>
            run({
              step: new WorkflowStep({
                queue: this.queue,
                workflowId: this.workflowId,
                workflowJobId: this.workflowJobId,
                stepNamePrefix: name,
              }),
              span,
            }),
        )

        await this.updateStepData(name, {
          type: 'do',
          result,
          attempt: initialAttempt + attempt,
        })

        return result
      },
      {
        ...options?.retry,
        retries: (options?.retry?.retries ?? 0) - initialAttempt,
        onFailedAttempt: async (ctx) => {
          await this.updateStepData(name, {
            type: 'do',
            attempt: initialAttempt + ctx.attemptNumber,
          })
          return options?.retry?.onFailedAttempt?.(ctx)
        },
      },
    )
  }

  async wait(stepName: string, durationMs: number) {
    const name = this.addNamePrefix(stepName)

    const job = await this.getWorkflowJob()
    const existingStepData = await this.getStepData('wait', name)

    const now = Date.now()
    const stepData = existingStepData ?? {
      type: 'wait',
      durationMs,
      startedAt: now,
    }

    await this.updateStepData(name, stepData)

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
        const remainingMs = Math.max(0, stepData.startedAt + stepData.durationMs - now)

        // update progress every 15s to avoid job being marked as stalled
        const interval = setInterval(() => {
          void job.updateProgress(name)
        }, 15_000)

        await setTimeout(remainingMs)
        clearInterval(interval)
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
    const job = await this.getWorkflowJob()

    const jobData = deserialize(job.data)
    const stepData = jobData.stepData[stepName]

    if (stepData && stepData.type !== type)
      throw new Error(`Step "${stepName}" is of type "${stepData.type}", expected "${type}"`)

    return stepData as Extract<WorkflowStepData, { type: T }> | undefined
  }

  private async updateStepData(stepName: string, data: WorkflowStepData) {
    const job = await this.getWorkflowJob()

    const jobData = deserialize(job.data)
    jobData.stepData[stepName] = data

    await Promise.all([job.updateData(serialize(jobData)), job.updateProgress(stepName)])
  }

  private async getWorkflowJob() {
    const job = await this.queue.getJob(this.workflowJobId)
    if (!job)
      throw new UnrecoverableError(`Could not find workflow job with ID ${this.workflowJobId}`)
    return job
  }
}

export interface WorkflowStepOptions {
  retry?: Options
}
