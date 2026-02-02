import type { Span } from '@opentelemetry/api'
import type { Options } from 'p-retry'
import type { WorkflowQueueInternal } from './types'
import { setTimeout } from 'node:timers/promises'
import Mutex from 'p-mutex'
import pRetry from 'p-retry'
import { deserialize, serialize } from './serializer'
import { Settings } from './settings'
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
  private updateStepDataMutex = new Mutex()

  constructor(opts: {
    queue: WorkflowQueueInternal<unknown>
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
    if (stepData && 'result' in stepData) {
      Settings.logger?.debug?.(
        `[${this.workflowId}/${this.workflowJobId}] Step '${name}' already completed, returning cached result`,
      )
      return stepData.result as R
    }

    const initialAttempt = stepData?.attempt ?? 0
    await this.updateStepData(name, {
      type: 'do',
      attempt: initialAttempt,
    })

    Settings.logger?.debug?.(
      `[${this.workflowId}/${this.workflowJobId}] Running step '${name}' (attempt ${initialAttempt + 1})`,
    )
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

        Settings.logger?.debug?.(
          `[${this.workflowId}/${this.workflowJobId}] Completed step '${name}'`,
        )

        return result
      },
      {
        ...options?.retry,
        retries: Math.max((options?.retry?.retries ?? 0) - initialAttempt, 0),
        onFailedAttempt: async (ctx) => {
          Settings.logger?.debug?.(
            `[${this.workflowId}/${this.workflowJobId}] Step '${name}' error:`,
            ctx.error,
          )
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
        Settings.logger?.debug?.(
          `[${this.workflowId}/${this.workflowJobId}] Waiting in step '${name}' for ${durationMs} ms`,
        )
        const remainingMs = Math.max(0, stepData.startedAt + stepData.durationMs - now)
        await setTimeout(remainingMs)
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

  // TODO maybe update this to use lua script for atomic operation
  private async updateStepData(stepName: string, data: WorkflowStepData) {
    // prevent concurrent updates to step data which could lead to race conditions and lost updates
    await this.updateStepDataMutex.withLock(async () => {
      const job = await this.getWorkflowJob()

      const jobData = deserialize(job.data)
      jobData.stepData[stepName] = data

      await job.updateData(serialize(jobData))
    })
  }

  private async getWorkflowJob() {
    const job = await this.queue.getJob(this.workflowJobId)
    if (!job) throw new Error(`Could not find workflow job with ID ${this.workflowJobId}`)
    return job
  }
}

export interface WorkflowStepOptions {
  retry?: Options
}
