import type { QueueEvents } from 'bullmq'
import type { WorkflowJobInternal } from './types'
import { deserialize } from './serializer'

export class WorkflowJob<Output> {
  private job
  private queueEvents

  constructor(opts: { job: WorkflowJobInternal<unknown, Output>; queueEvents: QueueEvents }) {
    this.job = opts.job
    this.queueEvents = opts.queueEvents
  }

  async wait(timeoutMs?: number) {
    const result = await this.job.waitUntilFinished(this.queueEvents, timeoutMs)

    return deserialize(result)
  }
}
