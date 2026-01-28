import type { Job } from 'groupmq'
import type { Serialized } from './serializer'
import { setInterval } from 'node:timers/promises'
import { deserialize } from './serializer'

export class WorkflowJob<Output> {
  private job
  groupId
  id

  constructor(opts: { job: Job<unknown> }) {
    this.job = opts.job
    this.groupId = opts.job.groupId
    this.id = opts.job.id
  }

  async wait(timeoutMs?: number) {
    if (this.job.finishedOn) {
      const returnValue = this.job.returnvalue as Serialized<Output> | undefined
      return returnValue && deserialize(returnValue)
    }

    for await (const _ of setInterval(1000, undefined, {
      signal: timeoutMs ? AbortSignal.timeout(timeoutMs) : undefined,
    })) {
      const updatedJob = await this.job.queue.getJob(this.job.id).catch(() => null)

      // deleted after completion
      if (!updatedJob) return
      if (updatedJob.finishedOn) {
        const returnValue = updatedJob.returnvalue as Serialized<Output> | undefined
        return returnValue && deserialize(returnValue)
      }
    }
  }
}
