import type { Queue } from './queue'
import { deserialize } from './serializer'

export class WorkflowJob<Output> {
  private queue
  private jobId
  groupId
  id

  constructor(opts: { queue: Queue; jobId: string; groupId: string }) {
    this.queue = opts.queue
    this.jobId = opts.jobId
    this.groupId = opts.groupId
    this.id = opts.jobId
  }

  /**
   * Block until the job finishes and return its output. Propagates the workflow's own failure,
   * `TimeoutError` (if `timeoutMs` elapses), and `ResultExpiredError` (past the result window).
   */
  async wait(timeoutMs?: number): Promise<Output> {
    const raw = await this.queue.wait(this.jobId, { timeoutMs })
    return deserialize<Output>(raw)
  }
}
