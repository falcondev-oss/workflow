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

export class WorkflowStep<Progress = never> {
  private workflowId
  private queue
  private workflowJobId
  private stepNamePrefix
  private signal
  private stepPromises
  private logger
  private memo
  private recordStepDuration

  constructor(opts: {
    queue: WorkflowQueueInternal
    workflowJobId: string
    workflowId: string
    stepNamePrefix?: string
    signal: AbortSignal
    stepPromises: Set<Promise<any>>
    memo: Map<string, string>
    recordStepDuration?: (step: string, durationMs: number) => void
  }) {
    this.queue = opts.queue
    this.workflowJobId = opts.workflowJobId
    this.workflowId = opts.workflowId
    this.stepNamePrefix = opts.stepNamePrefix ? `${opts.stepNamePrefix}|` : ''
    this.signal = opts.signal
    this.stepPromises = opts.stepPromises
    this.logger = opts.queue.logger
    this.memo = opts.memo
    this.recordStepDuration = opts.recordStepDuration
  }

  private addNamePrefix(name: string) {
    return `${this.stepNamePrefix}${name}`
  }

  private getStepData<T extends WorkflowStepData['type']>(type: T, stepName: string) {
    const raw = this.memo.get(stepName)
    if (raw === undefined) return

    const stepData = deserialize<WorkflowStepData>(raw)
    if (stepData.type !== type)
      throw new Error(`Step "${stepName}" is of type "${stepData.type}", expected "${type}"`)

    return stepData as Extract<WorkflowStepData, { type: T }>
  }

  private async updateStepData(stepName: string, data: WorkflowStepData) {
    const raw = serialize(data)
    await this.queue.setStepData(this.workflowJobId, stepName, raw)
    this.memo.set(stepName, raw)
  }

  async do<R>(
    stepName: string,
    run: (ctx: { step: WorkflowStep<Progress>; span: Span; signal: AbortSignal }) => R,
  ) {
    const name = this.addNamePrefix(stepName)

    const stepData = this.getStepData('do', name)
    if (stepData && 'result' in stepData) {
      this.logger?.debug?.(
        `[${this.workflowId}/${this.workflowJobId}] Step '${name}' already completed, returning cached result`,
      )
      return stepData.result as R
    }

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
        const start = performance.now()
        let result: Awaited<R>
        try {
          result = await run({
            step: new WorkflowStep<Progress>({
              queue: this.queue,
              workflowId: this.workflowId,
              workflowJobId: this.workflowJobId,
              stepNamePrefix: name,
              signal: this.signal,
              stepPromises: this.stepPromises,
              memo: this.memo,
              recordStepDuration: this.recordStepDuration,
            }),
            span,
            signal: this.signal,
          })
        } finally {
          this.recordStepDuration?.(name, performance.now() - start)
        }

        await this.updateStepData(name, { type: 'do', result })

        this.logger?.debug?.(`[${this.workflowId}/${this.workflowJobId}] Completed step '${name}'`)

        return result
      },
    )

    this.stepPromises.add(promise)
    return promise.finally(() => this.stepPromises.delete(promise))
  }

  async progress(data: Progress): Promise<void> {
    await this.queue.publish(this.workflowJobId, { type: 'progress', data: serialize(data) })
  }

  async wait(stepName: string, durationMs: number) {
    const name = this.addNamePrefix(stepName)

    const existingStepData = this.getStepData('wait', name)

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
}
